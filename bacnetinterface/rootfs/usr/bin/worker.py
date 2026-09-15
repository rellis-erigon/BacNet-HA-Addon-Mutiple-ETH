"""Per-interface BACnet worker process.

Each worker runs in its own process with its own asyncio event loop and
bacpypes3 Application bound to a single network interface. It communicates
with the supervisor via multiprocessing queues defined in ipc.py.
"""

import asyncio
import json
import logging
import multiprocessing as mp
import traceback
from logging import Formatter, StreamHandler

from bacpypes3.basetypes import Null, Segmentation, ServicesSupported
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import IPv4Address
from bacpypes3.primitivedata import ObjectIdentifier

from ipc import CmdType, Command, DataMessage, DataType

LOGGER = logging.getLogger("bacnet.worker")


def run_worker(
    interface_config: dict,
    global_config: dict,
    cmd_queue: mp.Queue,
    data_queue: mp.Queue,
    token: str | None,
):
    """Entry point for a worker process."""
    iface_name = interface_config.get("name", "unknown")

    formatter = Formatter(
        f"[%(asctime)-8s]|%(levelname)-8s |[{iface_name}] %(filename)-18s->%(funcName)-36s: %(message)s",
        datefmt="%H:%M:%S",
    )

    loglevel = global_config.get("loglevel", "INFO")

    stream_handler = StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(loglevel)

    LOGGER.addHandler(stream_handler)
    LOGGER.setLevel("DEBUG")

    try:
        asyncio.run(_worker_main(interface_config, global_config, cmd_queue, data_queue, token))
    except KeyboardInterrupt:
        LOGGER.info(f"Worker [{iface_name}] interrupted")
    except Exception as err:
        LOGGER.error(f"Worker [{iface_name}] fatal error: {err}\n{traceback.format_exc()}")
        data_queue.put(DataMessage(
            msg_type=DataType.ERROR,
            interface_name=iface_name,
            payload=str(err),
        ))


async def _worker_main(
    interface_config: dict,
    global_config: dict,
    cmd_queue: mp.Queue,
    data_queue: mp.Queue,
    token: str | None,
):
    """Async main loop for one BACnet interface worker."""
    from BACnetIOHandler import BACnetIOHandler, ObjectManager
    from const import subscribable_objects

    iface_name = interface_config["name"]
    ipv4_address = IPv4Address(interface_config["cidr"])
    object_identifier = interface_config["objectIdentifier"]
    object_name = interface_config["objectName"]
    foreign_ip = interface_config.get("foreignBBMD")
    foreign_ttl = interface_config.get("foreignTTL", 255)
    devices_setup = interface_config.get("devices_setup", [])
    entity_list = interface_config.get("entity_list", [])

    vendor_id = global_config.get("vendorID", 15)
    segmentation_supported = global_config.get("segmentation", "segmentedBoth")
    max_apdu = global_config.get("maxApduLenghtAccepted", 1476)
    max_segments = global_config.get("maxSegmentsAccepted", 64)
    default_write_prio = global_config.get("defaultPriority", 15)

    LOGGER.info(
        f"Worker [{iface_name}] starting: "
        f"ID={object_identifier}, IP={ipv4_address}, foreign={foreign_ip}"
    )

    this_device = DeviceObject(
        objectIdentifier=ObjectIdentifier(f"device,{object_identifier}"),
        objectName=object_name,
        description=f"BACnet Multi-Interface Add-on [{iface_name}]",
        vendorIdentifier=int(vendor_id),
        segmentationSupported=Segmentation(segmentation_supported),
        maxApduLengthAccepted=int(max_apdu),
        maxSegmentsAccepted=int(max_segments),
    )

    if foreign_ip == "-":
        foreign_ip = None

    update_event = asyncio.Event()

    app = BACnetIOHandler(
        device=this_device,
        local_ip=ipv4_address,
        foreign_ip=foreign_ip,
        ttl=int(foreign_ttl),
        update_event=update_event,
        addon_device_config=devices_setup,
    )

    _object_manager = ObjectManager(
        app=app, entity_list=entity_list if entity_list else None, api_token=token
    )

    app.asap.maxApduLengthAccepted = int(max_apdu)
    app.asap.segmentationSupported = Segmentation(segmentation_supported)
    app.asap.maxSegmentsAccepted = int(max_segments)
    app.asap.apduTimeout = int(5000)
    app.subscription_list = subscribable_objects

    data_queue.put(DataMessage(
        msg_type=DataType.STARTUP_COMPLETE,
        interface_name=iface_name,
    ))

    shutdown_event = asyncio.Event()

    cmd_task = asyncio.create_task(
        _command_listener(app, cmd_queue, data_queue, iface_name, default_write_prio, shutdown_event)
    )
    sync_task = asyncio.create_task(
        _dict_sync_task(app, data_queue, iface_name, update_event)
    )
    sub_sync_task = asyncio.create_task(
        _subscription_sync_task(app, data_queue, iface_name)
    )

    await shutdown_event.wait()

    LOGGER.info(f"Worker [{iface_name}] shutting down")
    cmd_task.cancel()
    sync_task.cancel()
    sub_sync_task.cancel()

    try:
        app.bacnet_device_sqlite.commit()
        app.bacnet_device_sqlite.close()
        await app.end_subscription_tasks()
        app.close()
    except Exception as err:
        LOGGER.warning(f"Worker [{iface_name}] cleanup error: {err}")


async def _command_listener(
    app,
    cmd_queue: mp.Queue,
    data_queue: mp.Queue,
    iface_name: str,
    default_write_prio: int,
    shutdown_event: asyncio.Event,
):
    """Listen for commands from the supervisor and execute them."""
    from bacpypes3.apdu import AbortPDU, ErrorPDU, ErrorRejectAbortNack, RejectPDU
    from bacpypes3.basetypes import Null
    from bacpypes3.pdu import Address
    from datetime import datetime

    loop = asyncio.get_event_loop()

    while not shutdown_event.is_set():
        try:
            cmd = await loop.run_in_executor(None, cmd_queue.get, True, 0.5)
        except Exception:
            continue

        if not isinstance(cmd, Command):
            continue

        try:
            if cmd.cmd_type == CmdType.SHUTDOWN:
                shutdown_event.set()
                break

            elif cmd.cmd_type == CmdType.WRITE_PROPERTY:
                p = cmd.payload
                device_id = p["device_id"]
                object_id = p["object_id"]
                property_id = p["property_id"]
                property_val = p.get("value")
                array_index = p.get("array_index")
                priority = p.get("priority") or default_write_prio

                if property_val is None:
                    property_val = Null("null")

                try:
                    response = await app.write_property(
                        address=app.dev_to_addr(device_id),
                        objid=object_id,
                        prop=property_id,
                        value=property_val,
                        array_index=array_index,
                        priority=priority,
                    )
                    LOGGER.info(f"Write response: {response if response else 'Acknowledged'}")

                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"result": "success"},
                            request_id=cmd.request_id,
                        ))
                except (AbortPDU, ErrorPDU, RejectPDU, ErrorRejectAbortNack) as err:
                    LOGGER.error(f"Write error: {err}")
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"error": str(err)},
                            request_id=cmd.request_id,
                        ))
                except Exception as err:
                    LOGGER.error(f"Write error: {err}")
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"error": str(err)},
                            request_id=cmd.request_id,
                        ))

            elif cmd.cmd_type == CmdType.READ_PROPERTY:
                p = cmd.payload
                try:
                    from fastapi.encoders import jsonable_encoder
                    address = app.dev_to_addr(p["device_id"])
                    result = await app.read_property(
                        address, p["object_id"], p["property_id"], p.get("array_index")
                    )
                    data_queue.put(DataMessage(
                        msg_type=DataType.COMMAND_RESPONSE,
                        interface_name=iface_name,
                        payload={"result": jsonable_encoder(result)},
                        request_id=cmd.request_id,
                    ))
                except Exception as err:
                    data_queue.put(DataMessage(
                        msg_type=DataType.COMMAND_RESPONSE,
                        interface_name=iface_name,
                        payload={"error": str(err)},
                        request_id=cmd.request_id,
                    ))

            elif cmd.cmd_type == CmdType.TIME_SYNC:
                p = cmd.payload
                try:
                    device_id = p.get("device_id")
                    dt = p.get("date_time")
                    if device_id:
                        app.time_sync(address=app.dev_to_addr(device_id), date_time=dt)
                    else:
                        app.time_sync(date_time=dt)
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"result": "success"},
                            request_id=cmd.request_id,
                        ))
                except Exception as err:
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"error": str(err)},
                            request_id=cmd.request_id,
                        ))

            elif cmd.cmd_type == CmdType.UTC_TIME_SYNC:
                p = cmd.payload
                try:
                    device_id = p.get("device_id")
                    dt = p.get("date_time")
                    if device_id:
                        app.utc_time_sync(address=app.dev_to_addr(device_id), date_time=dt)
                    else:
                        app.utc_time_sync(date_time=dt)
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"result": "success"},
                            request_id=cmd.request_id,
                        ))
                except Exception as err:
                    if cmd.request_id:
                        data_queue.put(DataMessage(
                            msg_type=DataType.COMMAND_RESPONSE,
                            interface_name=iface_name,
                            payload={"error": str(err)},
                            request_id=cmd.request_id,
                        ))

            elif cmd.cmd_type == CmdType.SUBSCRIBE:
                p = cmd.payload
                await app.create_subscription_task(
                    device_identifier=p["device_id"],
                    object_identifier=p["object_id"],
                    confirmed_notifications=p.get("confirmed", True),
                    lifetime=p.get("lifetime"),
                )
                if cmd.request_id:
                    await asyncio.sleep(0.5)
                    sub_info = _get_subscription_info(app, iface_name)
                    data_queue.put(DataMessage(
                        msg_type=DataType.COMMAND_RESPONSE,
                        interface_name=iface_name,
                        payload={"result": "subscribed", "subscriptions": sub_info},
                        request_id=cmd.request_id,
                    ))

            elif cmd.cmd_type == CmdType.UNSUBSCRIBE:
                p = cmd.payload
                task_name = f"{p['device_id'][0].attr}:{p['device_id'][1]},{p['object_id'][0].attr}:{p['object_id'][1]}"
                for task in app.subscription_tasks:
                    if task_name in task.get_name():
                        task.cancel()
                        break
                if cmd.request_id:
                    data_queue.put(DataMessage(
                        msg_type=DataType.COMMAND_RESPONSE,
                        interface_name=iface_name,
                        payload={"result": "unsubscribed"},
                        request_id=cmd.request_id,
                    ))

            elif cmd.cmd_type == CmdType.GET_SUBSCRIPTIONS:
                sub_info = _get_subscription_info(
                    app, iface_name,
                    device_filter=cmd.payload.get("device_id") if cmd.payload else None,
                    object_filter=cmd.payload.get("object_id") if cmd.payload else None,
                )
                data_queue.put(DataMessage(
                    msg_type=DataType.COMMAND_RESPONSE,
                    interface_name=iface_name,
                    payload={"subscriptions": sub_info},
                    request_id=cmd.request_id,
                ))

            elif cmd.cmd_type == CmdType.WHO_IS:
                await app.who_is()

            elif cmd.cmd_type == CmdType.I_AM:
                app.i_am()

            elif cmd.cmd_type == CmdType.READ_ALL:
                for device_id in app.bacnet_device_dict:
                    services_supported = app.bacnet_device_dict[device_id][device_id].get(
                        "protocolServicesSupported", ServicesSupported()
                    )
                    if services_supported["read-property-multiple"] == 1:
                        await app.read_multiple_objects_periodically(device_identifier=device_id)
                    else:
                        await app.read_objects_periodically(device_identifier=device_id)

        except Exception as err:
            LOGGER.error(f"Command handler error ({cmd.cmd_type}): {err}")
            if cmd.request_id:
                data_queue.put(DataMessage(
                    msg_type=DataType.COMMAND_RESPONSE,
                    interface_name=iface_name,
                    payload={"error": str(err)},
                    request_id=cmd.request_id,
                ))


def _get_subscription_info(app, iface_name, device_filter=None, object_filter=None):
    """Extract subscription task info for reporting to the supervisor."""
    subs = []
    for task in app.subscription_tasks:
        try:
            context = task.get_context()
            items = list(context.items())

            device_id = app.identifier_to_string(
                next((v for k, v in items if k.name == "device_context"), None)
            )
            if device_filter and device_id != device_filter:
                continue

            object_id = app.identifier_to_string(
                next((v for k, v in items if k.name == "object_context"), None)
            )
            if object_filter and object_id != object_filter:
                continue

            confirmation = next(
                (v for k, v in items if k.name == "confirmation_context"), None
            )
            lifetime = next(
                (v for k, v in items if k.name == "lifetime_context"), None
            )
            lifetime_remaining = next(
                (v for k, v in items if k.name == "lifetime_remaining_context"), None
            )

            import asyncio
            if lifetime_remaining is not None:
                lifetime_remaining = round(
                    max(0, lifetime_remaining - asyncio.get_event_loop().time()), 1
                )

            subs.append({
                "interface": iface_name,
                "device_id": device_id,
                "object_id": object_id,
                "confirmation": confirmation,
                "lifetime": lifetime,
                "lifetime_remaining": lifetime_remaining,
            })
        except Exception:
            continue
    return subs


async def _dict_sync_task(
    app,
    data_queue: mp.Queue,
    iface_name: str,
    update_event: asyncio.Event,
):
    """Periodically send the worker's device dict to the supervisor."""
    try:
        while True:
            await update_event.wait()
            update_event.clear()

            try:
                from fastapi.encoders import jsonable_encoder
                serializable = jsonable_encoder(app.bacnet_device_dict)
            except Exception:
                serializable = dict(app.bacnet_device_dict)

            device_ids = set(app.bacnet_device_dict.keys())

            data_queue.put(DataMessage(
                msg_type=DataType.DEVICE_DICT_UPDATE,
                interface_name=iface_name,
                payload={
                    "device_dict": serializable,
                    "device_ids": device_ids,
                },
            ))

            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        LOGGER.debug(f"Dict sync task [{iface_name}] cancelled")


async def _subscription_sync_task(
    app,
    data_queue: mp.Queue,
    iface_name: str,
):
    """Periodically send subscription state to the supervisor."""
    try:
        while True:
            await asyncio.sleep(5)
            sub_info = _get_subscription_info(app, iface_name)
            data_queue.put(DataMessage(
                msg_type=DataType.SUBSCRIPTION_INFO,
                interface_name=iface_name,
                payload=sub_info,
            ))
    except asyncio.CancelledError:
        LOGGER.debug(f"Subscription sync task [{iface_name}] cancelled")
