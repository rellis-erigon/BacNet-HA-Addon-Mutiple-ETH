"""Main script for BACnet multi-interface add-on.

Phase 1: Runs a single BACnet interface using the new config schema.
The first enabled interface from the 'interfaces' list is used.
Phase 2 will add process-per-interface worker spawning.
"""

import asyncio
import json
import os
import signal
from datetime import datetime
from logging import Formatter, StreamHandler, getLogger
from logging.handlers import RotatingFileHandler

import uvicorn
import webAPI
from BACnetIOHandler import BACnetIOHandler, ObjectManager
from bacpypes3.basetypes import Null, Segmentation, ServicesSupported
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import IPv4Address
from bacpypes3.primitivedata import ObjectIdentifier
from const import LOGGER, subscribable_objects
from interface_discovery import discover_and_resolve
from webAPI import app as fastapi_app


def exception_handler(loop, context):
    try:
        LOGGER.exception(f'An uncaught error occurred: {context["exception"]}')
    except Exception:
        LOGGER.error("Tried to log error, but something went horribly wrong!!!")


async def updater_task(app, interval, event):
    try:
        while True:
            await event.wait()
            for device_id in app.bacnet_device_dict:
                services_supported = app.bacnet_device_dict[device_id][device_id].get(
                    "protocolServicesSupported", ServicesSupported()
                )
                if services_supported["read-property-multiple"] == 1:
                    await app.read_multiple_objects_periodically(
                        device_identifier=device_id
                    )
                else:
                    await app.read_objects_periodically(device_identifier=device_id)
            event.clear()
    except asyncio.CancelledError:
        LOGGER.warning("Updater task cancelled")


async def writer_task(app, write_queue, default_write_prio):
    from bacpypes3.apdu import AbortPDU, ErrorPDU, RejectPDU

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

            if property_val is None:
                property_val = Null("null")

            LOGGER.debug(
                f"Writing: {device_id}, {object_id}, {property_id}, {property_val}, {priority}"
            )

            try:
                response = await app.write_property(
                    address=app.dev_to_addr(device_id),
                    objid=object_id,
                    prop=property_id,
                    value=property_val,
                    array_index=array_index,
                    priority=priority,
                )
            except (AbortPDU, ErrorPDU, RejectPDU) as err:
                LOGGER.error(f"response: {err}")
                continue
            except Exception as err:
                LOGGER.error(f"response: {err}")
                continue

            LOGGER.info(f"response: {response if response else 'Acknowledged'}")

            await asyncio.sleep(0.1)

            read = await app.read_property(
                address=app.dev_to_addr(device_id),
                objid=object_id,
                prop=property_id,
                array_index=array_index,
            )
            LOGGER.info(f"Write result: {read}")

            app.dict_updater(
                device_identifier=device_id,
                object_identifier=object_id,
                property_identifier=property_id,
                property_value=property_val,
            )
    except Exception as err:
        LOGGER.error(f"Writer task error: {err}")
    except asyncio.CancelledError:
        LOGGER.warning("Writer task cancelled")


async def subscribe_handler_task(app, sub_queue):
    try:
        while True:
            queue_result = await sub_queue.get()
            device_identifier = queue_result[0]
            object_identifier = queue_result[1]
            notifications = queue_result[2]
            lifetime = queue_result[3]

            task_name = f"{device_identifier[0].attr}:{device_identifier[1]},{object_identifier[0].attr}:{object_identifier[1]}"

            for task in app.subscription_tasks:
                if task_name in task.get_name():
                    LOGGER.error(
                        f"Subscription for {device_identifier}, {object_identifier} already exists"
                    )
                    break
            else:
                await app.create_subscription_task(
                    device_identifier=device_identifier,
                    object_identifier=object_identifier,
                    confirmed_notifications=notifications,
                    lifetime=lifetime,
                )
    except asyncio.CancelledError:
        LOGGER.warning("Subscribe task cancelled")


async def unsubscribe_handler_task(app, unsub_queue):
    try:
        while True:
            queue_result = await unsub_queue.get()
            device_identifier = queue_result[0]
            object_identifier = queue_result[1]

            task_name = f"{device_identifier[0].attr}:{device_identifier[1]},{object_identifier[0].attr}:{object_identifier[1]}"

            for task in app.subscription_tasks:
                if task_name in task.get_name():
                    task.cancel()
                    break
            else:
                LOGGER.error("Subscription task does not exist")
    except asyncio.CancelledError:
        LOGGER.warning("Unsubscribe task cancelled")


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


async def main():
    options, token = get_configuration()

    loglevel = options.get("loglevel", "INFO")
    default_write_prio = options.get("defaultPriority", 15)
    vendor_id = options.get("vendorID", 15)
    segmentation_supported = options.get("segmentation", "segmentedBoth")
    max_apdu = options.get("maxApduLenghtAccepted", 1476)
    max_segments = options.get("maxSegmentsAccepted", 64)

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

    # Discover and resolve interfaces
    resolved_interfaces = discover_and_resolve(options)

    if not resolved_interfaces:
        LOGGER.error("No interfaces available — cannot start BACnet stack")
        return

    # Phase 1: use the first resolved interface
    iface = resolved_interfaces[0]
    if len(resolved_interfaces) > 1:
        LOGGER.warning(
            f"Multiple interfaces configured ({len(resolved_interfaces)}), "
            f"but Phase 1 only supports one. Using '{iface['name']}' ({iface['cidr']}). "
            f"Multi-interface support coming in Phase 2."
        )

    ipv4_address = IPv4Address(iface["cidr"])
    object_identifier = iface["objectIdentifier"]
    object_name = iface["objectName"]
    foreign_ip = iface.get("foreignBBMD")
    foreign_ttl = iface.get("foreignTTL", 255)
    devices_setup = iface.get("devices_setup", [])
    entity_list = iface.get("entity_list", [])

    LOGGER.info(
        f"Starting on interface '{iface['name']}': "
        f"ID={object_identifier}, Name={object_name}, IP={ipv4_address}, "
        f"max_apdu={max_apdu}, segments={max_segments}, "
        f"segmentation={segmentation_supported}, foreign_ip={foreign_ip}"
    )

    this_device = DeviceObject(
        objectIdentifier=ObjectIdentifier(f"device,{object_identifier}"),
        objectName=object_name,
        description=f"BACnet Multi-Interface Add-on [{iface['name']}]",
        vendorIdentifier=int(vendor_id),
        segmentationSupported=Segmentation(segmentation_supported),
        maxApduLengthAccepted=int(max_apdu),
        maxSegmentsAccepted=int(max_segments),
    )

    if foreign_ip == "-":
        foreign_ip = None

    app = BACnetIOHandler(
        device=this_device,
        local_ip=ipv4_address,
        foreign_ip=foreign_ip,
        ttl=int(foreign_ttl),
        update_event=webAPI.events.val_updated_event,
        addon_device_config=devices_setup,
    )

    object_manager = ObjectManager(
        app=app, entity_list=entity_list if entity_list else None, api_token=token
    )

    app.asap.maxApduLengthAccepted = int(max_apdu)
    app.asap.segmentationSupported = Segmentation(segmentation_supported)
    app.asap.maxSegmentsAccepted = int(max_segments)
    app.asap.apduTimeout = int(5000)
    app.subscription_list = subscribable_objects

    update_task = asyncio.create_task(
        updater_task(
            app=app,
            interval=int(500),
            event=webAPI.events.read_event,
        )
    )

    write_task = asyncio.create_task(
        writer_task(
            app=app,
            write_queue=webAPI.events.write_queue,
            default_write_prio=default_write_prio,
        )
    )

    sub_task = asyncio.create_task(
        subscribe_handler_task(app=app, sub_queue=webAPI.events.sub_queue)
    )

    unsub_task = asyncio.create_task(
        unsubscribe_handler_task(app=app, unsub_queue=webAPI.events.unsub_queue)
    )

    webAPI.sub_list = app.subscription_tasks
    webAPI.bacnet_device_dict = app.bacnet_device_dict
    webAPI.bacnet_application = app
    webAPI.who_is_func = app.who_is
    webAPI.i_am_func = app.i_am
    webAPI.events.startup_complete_event = app.startup_complete

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

    if app:
        app.bacnet_device_sqlite.commit()
        app.bacnet_device_sqlite.close()
        update_task.cancel()
        write_task.cancel()
        sub_task.cancel()
        unsub_task.cancel()
        await app.end_subscription_tasks()
        app.close()


if __name__ == "__main__":
    asyncio.run(main())
