"""Supervisor for BACnet multi-interface add-on.

Spawns one worker process per enabled interface, each running its own
bacpypes3 Application bound to a single network adapter. The supervisor
runs the FastAPI web server and aggregates device data from all workers.
"""

import asyncio
import json
import multiprocessing as mp
import os
import signal
from datetime import datetime
from logging import Formatter, StreamHandler
from logging.handlers import RotatingFileHandler

import uvicorn
import webAPI
from bacpypes3.primitivedata import ObjectIdentifier
from const import LOGGER
from interface_discovery import discover_and_resolve
from ipc import (
    CmdType,
    Command,
    DataMessage,
    DataType,
    DeviceRouter,
    WorkerHandle,
    new_request_id,
)
from webAPI import app as fastapi_app
from worker import run_worker


def get_configuration():
    try:
        with open("/data/options.json") as f:
            options = json.load(f)
    except Exception as err:
        LOGGER.warning(f"No options.json detected! {err}")
        options = {}

    try:
        with open("/usr/bin/auth_token.ini", "r") as auth_token:
            token = auth_token.read()
    except Exception as err:
        LOGGER.warning(f"No Token received! {err}")
        token = None

    return options, token


def handler_stop_signals(signum, frame):
    LOGGER.info("Shutting down!")


class Supervisor:
    """Manages worker processes and aggregates their data for the web API."""

    def __init__(self, resolved_interfaces, global_config, token):
        self.resolved_interfaces = resolved_interfaces
        self.global_config = global_config
        self.token = token
        self.router = DeviceRouter()
        self.merged_device_dict = {}
        self.worker_dicts = {}
        self._running = True
        self._pending_responses: dict[str, asyncio.Future] = {}

    def spawn_workers(self):
        for iface in self.resolved_interfaces:
            handle = WorkerHandle(
                name=iface["name"],
                interface_config=iface,
            )

            process = mp.Process(
                target=run_worker,
                args=(
                    iface,
                    self.global_config,
                    handle.cmd_queue,
                    handle.data_queue,
                    self.token,
                ),
                name=f"bacnet-{iface['name']}",
                daemon=True,
            )
            handle.process = process
            self.router.register_worker(handle)

            process.start()
            handle.is_alive = True
            LOGGER.info(f"Spawned worker for interface '{iface['name']}' (pid={process.pid})")

    async def send_and_wait(self, handle: WorkerHandle, cmd: Command, timeout: float = 10.0) -> dict:
        """Send a command and wait for its response."""
        req_id = new_request_id()
        cmd.request_id = req_id

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self._pending_responses[req_id] = future

        handle.cmd_queue.put(cmd)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return {"error": "Request timed out"}
        finally:
            self._pending_responses.pop(req_id, None)

    async def data_collector(self):
        """Poll all worker data queues and merge device dicts."""
        while self._running:
            for handle in self.router.get_all_workers():
                while True:
                    try:
                        msg = handle.data_queue.get_nowait()
                    except Exception:
                        break

                    if not isinstance(msg, DataMessage):
                        continue

                    if msg.msg_type == DataType.DEVICE_DICT_UPDATE:
                        payload = msg.payload
                        self.worker_dicts[msg.interface_name] = payload.get("device_dict", {})
                        device_ids = payload.get("device_ids", set())
                        self.router.update_devices(msg.interface_name, device_ids)
                        handle.device_ids = device_ids
                        self._rebuild_merged_dict()

                    elif msg.msg_type == DataType.STARTUP_COMPLETE:
                        LOGGER.info(f"Worker '{msg.interface_name}' startup complete")

                    elif msg.msg_type == DataType.ERROR:
                        LOGGER.error(f"Worker '{msg.interface_name}' error: {msg.payload}")

                    elif msg.msg_type == DataType.SUBSCRIPTION_INFO:
                        handle.subscriptions = msg.payload or []

                    elif msg.msg_type == DataType.COMMAND_RESPONSE:
                        future = self._pending_responses.get(msg.request_id)
                        if future and not future.done():
                            future.set_result(msg.payload or {})

            self._check_worker_health()
            await asyncio.sleep(0.1)

    def _rebuild_merged_dict(self):
        merged = {}
        for iface_name, device_dict in self.worker_dicts.items():
            for device_id, device_data in device_dict.items():
                if device_id in merged:
                    merged[device_id].update(device_data)
                else:
                    merged[device_id] = dict(device_data)
        self.merged_device_dict = merged
        webAPI.bacnet_device_dict = self.merged_device_dict
        webAPI.events.val_updated_event.set()

    def _check_worker_health(self):
        for handle in self.router.get_all_workers():
            if handle.process and not handle.process.is_alive() and handle.is_alive:
                LOGGER.error(
                    f"Worker '{handle.name}' died (exit code {handle.process.exitcode})"
                )
                handle.is_alive = False

    def route_write(self, device_id_str, object_id, property_id, value, array_index, priority):
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            LOGGER.warning(f"No worker owns device {device_id_str}, broadcasting write")
            self.router.broadcast(Command(
                cmd_type=CmdType.WRITE_PROPERTY,
                payload={
                    "device_id": ObjectIdentifier(device_id_str),
                    "object_id": object_id,
                    "property_id": property_id,
                    "value": value,
                    "array_index": array_index,
                    "priority": priority,
                },
            ))
            return

        handle.cmd_queue.put(Command(
            cmd_type=CmdType.WRITE_PROPERTY,
            payload={
                "device_id": ObjectIdentifier(device_id_str),
                "object_id": object_id,
                "property_id": property_id,
                "value": value,
                "array_index": array_index,
                "priority": priority,
            },
        ))

    async def route_read_property(self, device_id_str, object_id, property_id, array_index=None):
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            return {"error": f"No worker owns device {device_id_str}"}

        return await self.send_and_wait(handle, Command(
            cmd_type=CmdType.READ_PROPERTY,
            payload={
                "device_id": ObjectIdentifier(device_id_str),
                "object_id": object_id,
                "property_id": property_id,
                "array_index": array_index,
            },
        ))

    async def route_write_v2(self, device_id, object_id, property_id, value, array_index, priority):
        device_id_str = f"{device_id[0].attr}:{device_id[1]}"
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            return {"error": f"No worker owns device {device_id_str}"}

        return await self.send_and_wait(handle, Command(
            cmd_type=CmdType.WRITE_PROPERTY,
            payload={
                "device_id": device_id,
                "object_id": object_id,
                "property_id": property_id,
                "value": value,
                "array_index": array_index,
                "priority": priority,
            },
        ))

    async def route_time_sync(self, device_id, date_time, utc=False):
        cmd_type = CmdType.UTC_TIME_SYNC if utc else CmdType.TIME_SYNC
        payload = {"date_time": date_time}
        if device_id:
            device_id_str = device_id if isinstance(device_id, str) else f"{device_id[0].attr}:{device_id[1]}"
            handle = self.router.get_worker_for_device(device_id_str)
            if handle:
                payload["device_id"] = device_id
                return await self.send_and_wait(handle, Command(cmd_type=cmd_type, payload=payload))
            return {"error": f"No worker owns device {device_id_str}"}

        results = []
        for handle in self.router.get_all_workers():
            result = await self.send_and_wait(handle, Command(cmd_type=cmd_type, payload=payload))
            results.append(result)
        return {"result": "success", "workers": len(results)}

    async def get_subscriptions(self, device_id=None, object_id=None):
        all_subs = []
        for handle in self.router.get_all_workers():
            result = await self.send_and_wait(handle, Command(
                cmd_type=CmdType.GET_SUBSCRIPTIONS,
                payload={"device_id": device_id, "object_id": object_id},
            ))
            subs = result.get("subscriptions", [])
            all_subs.extend(subs)
        return all_subs

    async def route_subscribe_v2(self, device_id, object_id, confirmed, lifetime):
        device_id_str = f"{device_id[0].attr}:{device_id[1]}"
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            return {"error": f"No worker owns device {device_id_str}"}

        return await self.send_and_wait(handle, Command(
            cmd_type=CmdType.SUBSCRIBE,
            payload={
                "device_id": device_id,
                "object_id": object_id,
                "confirmed": confirmed,
                "lifetime": lifetime,
            },
        ))

    async def route_unsubscribe_v2(self, device_id, object_id):
        device_id_str = f"{device_id[0].attr}:{device_id[1]}"
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            return {"error": f"No worker owns device {device_id_str}"}

        return await self.send_and_wait(handle, Command(
            cmd_type=CmdType.UNSUBSCRIBE,
            payload={"device_id": device_id, "object_id": object_id},
        ))

    def route_subscribe(self, device_id, object_id, confirmed, lifetime):
        device_id_str = f"{device_id[0].attr}:{device_id[1]}"
        handle = self.router.get_worker_for_device(device_id_str)
        if not handle:
            self.router.broadcast(Command(
                cmd_type=CmdType.SUBSCRIBE,
                payload={
                    "device_id": device_id,
                    "object_id": object_id,
                    "confirmed": confirmed,
                    "lifetime": lifetime,
                },
            ))
            return
        handle.cmd_queue.put(Command(
            cmd_type=CmdType.SUBSCRIBE,
            payload={
                "device_id": device_id,
                "object_id": object_id,
                "confirmed": confirmed,
                "lifetime": lifetime,
            },
        ))

    def route_unsubscribe(self, device_id, object_id):
        device_id_str = f"{device_id[0].attr}:{device_id[1]}"
        handle = self.router.get_worker_for_device(device_id_str)
        if handle:
            handle.cmd_queue.put(Command(
                cmd_type=CmdType.UNSUBSCRIBE,
                payload={"device_id": device_id, "object_id": object_id},
            ))
        else:
            self.router.broadcast(Command(
                cmd_type=CmdType.UNSUBSCRIBE,
                payload={"device_id": device_id, "object_id": object_id},
            ))

    def broadcast_who_is(self):
        self.router.broadcast(Command(cmd_type=CmdType.WHO_IS))

    def broadcast_i_am(self):
        self.router.broadcast(Command(cmd_type=CmdType.I_AM))

    def broadcast_read_all(self):
        self.router.broadcast(Command(cmd_type=CmdType.READ_ALL))

    def shutdown_workers(self):
        self._running = False
        self.router.broadcast(Command(cmd_type=CmdType.SHUTDOWN))
        for handle in self.router.get_all_workers():
            if handle.process and handle.process.is_alive():
                handle.process.join(timeout=5)
                if handle.process.is_alive():
                    LOGGER.warning(f"Force-killing worker '{handle.name}'")
                    handle.process.terminate()


class SupervisorProxy:
    """Mimics the BACnetIOHandler interface for webAPI v2 endpoints.

    Instead of calling bacpypes3 directly, routes operations through
    the supervisor's IPC to the correct worker process.
    """

    def __init__(self, supervisor: Supervisor):
        self._supervisor = supervisor
        self.subscription_tasks = []

    def identifier_to_string(self, ident):
        if ident is None:
            return None
        if hasattr(ident, '__iter__') and len(ident) == 2:
            return f"{ident[0].attr}:{ident[1]}" if hasattr(ident[0], 'attr') else f"{ident[0]}:{ident[1]}"
        return str(ident)

    def dev_to_addr(self, device_id):
        return device_id

    async def read_property(self, address, objid, prop, array_index=None):
        device_id_str = str(address) if not isinstance(address, str) else address
        if hasattr(address, '__iter__') and len(address) == 2 and hasattr(address[0], 'attr'):
            device_id_str = f"{address[0].attr}:{address[1]}"
        result = await self._supervisor.route_read_property(
            device_id_str, objid, prop, array_index
        )
        if "error" in result:
            raise Exception(result["error"])
        return result.get("result")

    async def write_property(self, address, objid, prop, value, array_index=None, priority=None):
        if hasattr(address, '__iter__') and len(address) == 2 and hasattr(address[0], 'attr'):
            device_id = address
        else:
            device_id = ObjectIdentifier(str(address))

        result = await self._supervisor.route_write_v2(
            device_id, objid, prop, value, array_index, priority
        )
        if "error" in result:
            raise Exception(result["error"])
        return result.get("result")

    def time_sync(self, address=None, date_time=None):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self._supervisor.route_time_sync(address, date_time, utc=False))

    def utc_time_sync(self, address=None, date_time=None):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self._supervisor.route_time_sync(address, date_time, utc=True))

    @property
    def vendor_info(self):
        from bacpypes3.object import get_vendor_info
        return get_vendor_info(0)

    async def create_subscription_task(self, device_identifier, object_identifier,
                                        confirmed_notifications=None, lifetime=None):
        return await self._supervisor.route_subscribe_v2(
            device_identifier, object_identifier, confirmed_notifications, lifetime
        )


async def write_forwarder(supervisor, write_queue, default_write_prio):
    """Reads from the webAPI write queue and routes to the correct worker."""
    try:
        while True:
            queue_result = await write_queue.get()
            device_id = queue_result[0]
            object_id = queue_result[1]
            property_id = queue_result[2]
            property_val = queue_result[3]
            array_index = queue_result[4]
            priority = queue_result[5]

            if not priority:
                priority = default_write_prio

            device_id_str = f"{device_id[0].attr}:{device_id[1]}"
            supervisor.route_write(
                device_id_str, object_id, property_id, property_val, array_index, priority
            )
    except asyncio.CancelledError:
        LOGGER.warning("Write forwarder cancelled")


async def subscribe_forwarder(supervisor, sub_queue):
    """Reads from the webAPI subscribe queue and routes to the correct worker."""
    try:
        while True:
            queue_result = await sub_queue.get()
            device_identifier = queue_result[0]
            object_identifier = queue_result[1]
            notifications = queue_result[2]
            lifetime = queue_result[3]
            supervisor.route_subscribe(device_identifier, object_identifier, notifications, lifetime)
    except asyncio.CancelledError:
        LOGGER.warning("Subscribe forwarder cancelled")


async def unsubscribe_forwarder(supervisor, unsub_queue):
    """Reads from the webAPI unsubscribe queue and routes to the correct worker."""
    try:
        while True:
            queue_result = await unsub_queue.get()
            device_identifier = queue_result[0]
            object_identifier = queue_result[1]
            supervisor.route_unsubscribe(device_identifier, object_identifier)
    except asyncio.CancelledError:
        LOGGER.warning("Unsubscribe forwarder cancelled")


async def main():
    options, token = get_configuration()

    loglevel = options.get("loglevel", "INFO")
    default_write_prio = options.get("defaultPriority", 15)

    formatter = Formatter(
        "[%(asctime)-8s]|%(levelname)-8s |%(filename)-18s->%(funcName)-36s: %(message)s",
        datefmt="%H:%M:%S",
    )

    path_str = os.path.dirname(os.path.realpath(__file__))
    date_var = datetime.now().date()
    log_path = f"{path_str}/bacnet_addon-{date_var}.log"
    webAPI.log_path = log_path

    file_handler = RotatingFileHandler(
        filename=log_path, mode="w", maxBytes=15 * 1024 * 1024, backupCount=2
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel("DEBUG")
    LOGGER.addHandler(file_handler)

    stream_handler = StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(loglevel)
    LOGGER.addHandler(stream_handler)

    LOGGER.setLevel("DEBUG")

    resolved_interfaces = discover_and_resolve(options)

    if not resolved_interfaces:
        LOGGER.error("No interfaces available — cannot start BACnet stack")
        return

    global_config = {
        "vendorID": options.get("vendorID", 15),
        "segmentation": options.get("segmentation", "segmentedBoth"),
        "maxApduLenghtAccepted": options.get("maxApduLenghtAccepted", 1476),
        "maxSegmentsAccepted": options.get("maxSegmentsAccepted", 64),
        "defaultPriority": default_write_prio,
        "loglevel": loglevel,
    }

    LOGGER.info(
        f"Starting supervisor with {len(resolved_interfaces)} interface(s): "
        f"{[r['name'] + '=' + r['cidr'] for r in resolved_interfaces]}"
    )

    supervisor = Supervisor(resolved_interfaces, global_config, token)
    supervisor.spawn_workers()

    proxy = SupervisorProxy(supervisor)

    webAPI.bacnet_device_dict = supervisor.merged_device_dict
    webAPI.bacnet_application = proxy
    webAPI.supervisor_ref = supervisor

    async def who_is_wrapper():
        supervisor.broadcast_who_is()
        return True

    def i_am_wrapper():
        supervisor.broadcast_i_am()

    webAPI.who_is_func = who_is_wrapper
    webAPI.i_am_func = i_am_wrapper
    webAPI.sub_list = proxy.subscription_tasks

    webAPI.events.startup_complete_event.set()

    collector_task = asyncio.create_task(supervisor.data_collector())

    write_fwd_task = asyncio.create_task(
        write_forwarder(supervisor, webAPI.events.write_queue, default_write_prio)
    )
    sub_fwd_task = asyncio.create_task(
        subscribe_forwarder(supervisor, webAPI.events.sub_queue)
    )
    unsub_fwd_task = asyncio.create_task(
        unsubscribe_forwarder(supervisor, webAPI.events.unsub_queue)
    )

    def _on_read_all():
        supervisor.broadcast_read_all()

    original_read_event_set = webAPI.events.read_event.set
    def patched_read_event_set():
        original_read_event_set()
        _on_read_all()
    webAPI.events.read_event.set = patched_read_event_set

    if loglevel == "DEBUG":
        uvilog = "info"
    else:
        uvilog = loglevel.lower()

    config = uvicorn.Config(
        app=fastapi_app, host="127.0.0.1", port=7813, log_level=uvilog, log_config=None
    )

    server = uvicorn.Server(config)

    signal.signal(signal.SIGINT, handler_stop_signals)
    signal.signal(signal.SIGTERM, handler_stop_signals)

    await server.serve()

    LOGGER.info("Supervisor shutting down — stopping workers")
    supervisor.shutdown_workers()
    collector_task.cancel()
    write_fwd_task.cancel()
    sub_fwd_task.cancel()
    unsub_fwd_task.cancel()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    asyncio.run(main())
