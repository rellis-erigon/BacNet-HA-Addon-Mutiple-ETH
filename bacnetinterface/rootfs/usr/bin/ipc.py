"""Inter-process communication for BACnet multi-interface workers.

Messages flow between the supervisor (main process running the web API)
and per-interface worker processes. Each worker has its own pair of queues:
  - cmd_queue:  supervisor → worker  (commands: write, subscribe, who-is, …)
  - data_queue: worker → supervisor  (device dict snapshots, status updates)

Request-response commands carry a request_id; the worker sends back a
COMMAND_RESPONSE with the same id so the supervisor can match futures.
"""

import multiprocessing as mp
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional


class CmdType(Enum):
    WRITE_PROPERTY = auto()
    SUBSCRIBE = auto()
    UNSUBSCRIBE = auto()
    WHO_IS = auto()
    I_AM = auto()
    READ_ALL = auto()
    SHUTDOWN = auto()
    READ_PROPERTY = auto()
    TIME_SYNC = auto()
    UTC_TIME_SYNC = auto()
    GET_SUBSCRIPTIONS = auto()


class DataType(Enum):
    DEVICE_DICT_UPDATE = auto()
    STARTUP_COMPLETE = auto()
    STATUS = auto()
    ERROR = auto()
    SUBSCRIPTION_INFO = auto()
    COMMAND_RESPONSE = auto()


@dataclass
class Command:
    cmd_type: CmdType
    payload: Any = None
    request_id: str = ""


@dataclass
class DataMessage:
    msg_type: DataType
    interface_name: str
    payload: Any = None
    request_id: str = ""


@dataclass
class WorkerHandle:
    """Tracks a spawned worker process and its communication channels."""
    name: str
    process: Optional[mp.Process] = None
    cmd_queue: mp.Queue = field(default_factory=mp.Queue)
    data_queue: mp.Queue = field(default_factory=mp.Queue)
    interface_config: dict = field(default_factory=dict)
    device_ids: set = field(default_factory=set)
    subscriptions: list = field(default_factory=list)
    is_alive: bool = False


class DeviceRouter:
    """Routes API requests to the correct worker based on device ownership."""

    def __init__(self):
        self._device_to_worker: dict[str, str] = {}
        self._workers: dict[str, WorkerHandle] = {}

    def register_worker(self, handle: WorkerHandle):
        self._workers[handle.name] = handle

    def remove_worker(self, name: str):
        for dev_id, worker_name in list(self._device_to_worker.items()):
            if worker_name == name:
                del self._device_to_worker[dev_id]
        self._workers.pop(name, None)

    def update_devices(self, worker_name: str, device_ids: set[str]):
        for dev_id in list(self._device_to_worker.keys()):
            if self._device_to_worker[dev_id] == worker_name:
                del self._device_to_worker[dev_id]
        for dev_id in device_ids:
            self._device_to_worker[dev_id] = worker_name

    def get_worker_for_device(self, device_id: str) -> Optional[WorkerHandle]:
        worker_name = self._device_to_worker.get(device_id)
        if worker_name:
            return self._workers.get(worker_name)
        return None

    def get_all_workers(self) -> list[WorkerHandle]:
        return list(self._workers.values())

    def get_worker(self, name: str) -> Optional[WorkerHandle]:
        return self._workers.get(name)

    def broadcast(self, cmd: Command):
        for handle in self._workers.values():
            try:
                handle.cmd_queue.put_nowait(cmd)
            except Exception:
                pass

    def get_interface_status(self) -> list[dict]:
        """Return status of all workers for the API."""
        result = []
        for handle in self._workers.values():
            result.append({
                "name": handle.name,
                "is_alive": handle.is_alive,
                "device_count": len(handle.device_ids),
                "devices": sorted(handle.device_ids),
                "subscription_count": len(handle.subscriptions),
                "config": {
                    "ip": handle.interface_config.get("ip", ""),
                    "cidr": handle.interface_config.get("cidr", ""),
                    "objectIdentifier": handle.interface_config.get("objectIdentifier", 0),
                    "objectName": handle.interface_config.get("objectName", ""),
                },
                "pid": handle.process.pid if handle.process and handle.process.is_alive() else None,
            })
        return result


def new_request_id() -> str:
    return uuid.uuid4().hex[:12]
