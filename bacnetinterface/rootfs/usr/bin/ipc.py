"""Inter-process communication for BACnet multi-interface workers.

Messages flow between the supervisor (main process running the web API)
and per-interface worker processes. Each worker has its own pair of queues:
  - cmd_queue:  supervisor → worker  (commands: write, subscribe, who-is, …)
  - data_queue: worker → supervisor  (device dict snapshots, status updates)
"""

import multiprocessing as mp
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


class DataType(Enum):
    DEVICE_DICT_UPDATE = auto()
    STARTUP_COMPLETE = auto()
    STATUS = auto()
    ERROR = auto()
    SUBSCRIPTION_INFO = auto()


@dataclass
class Command:
    cmd_type: CmdType
    payload: Any = None


@dataclass
class DataMessage:
    msg_type: DataType
    interface_name: str
    payload: Any = None


@dataclass
class WorkerHandle:
    """Tracks a spawned worker process and its communication channels."""
    name: str
    process: Optional[mp.Process] = None
    cmd_queue: mp.Queue = field(default_factory=mp.Queue)
    data_queue: mp.Queue = field(default_factory=mp.Queue)
    interface_config: dict = field(default_factory=dict)
    device_ids: set = field(default_factory=set)
    is_alive: bool = False


class DeviceRouter:
    """Routes API requests to the correct worker based on device ownership.

    Each worker discovers BACnet devices on its interface. The router
    maintains a mapping of device_id → worker_name so the supervisor
    can forward write/subscribe requests to the right worker.
    """

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

    def broadcast(self, cmd: Command):
        for handle in self._workers.values():
            try:
                handle.cmd_queue.put_nowait(cmd)
            except Exception:
                pass
