# Changelog

## [0.3.0] - 2026-09-15

### Added
- `SupervisorProxy` class that mimics BACnetIOHandler interface for webAPI v2 endpoints
- Request-response IPC pattern with `request_id` and `asyncio.Future` matching
- New IPC commands: `READ_PROPERTY`, `TIME_SYNC`, `UTC_TIME_SYNC`, `GET_SUBSCRIPTIONS`
- `COMMAND_RESPONSE` data type for worker → supervisor replies
- Subscription state syncing from workers to supervisor (periodic + on-demand)
- `/apiv2/interfaces` endpoint — lists all interfaces with status, device count, config
- `/apiv2/interfaces/{name}` endpoint — detailed interface info with devices and subscriptions
- `/apiv2/interfaces/{name}/devices` endpoint — per-interface device listing
- `get_interface_status()` method on DeviceRouter for API consumption

### Changed
- webAPI v2 endpoints (read/write property, time sync, CoV subscribe/unsubscribe)
  now route through IPC to workers instead of calling bacpypes3 directly
- `ipc.py` extended with request-response protocol and new command types
- `worker.py` handles all new command types with response-aware error handling
- Workers periodically report subscription state to supervisor
- `webAPI.py` gains `supervisor_ref` global for interface status endpoints

### Notes
- Phase 3 (Device Merging & Proxy): Full API compatibility with multi-process
  architecture — all v1 and v2 endpoints work across multiple interfaces
- Phase 4 will add UI enhancements and documentation

## [0.2.0] - 2026-09-15

### Added
- Process-per-interface worker architecture (`worker.py`)
- IPC module (`ipc.py`) with typed Command/DataMessage protocol and DeviceRouter
- Supervisor in `main.py` that spawns workers and aggregates device data
- Write/subscribe/unsubscribe request routing from web API to correct worker
- Worker health monitoring with dead-process detection
- Automatic device-to-worker mapping for targeted command routing
- Broadcast fallback when device ownership is unknown

### Changed
- `main.py` rewritten from single-interface runner to multi-process supervisor
- Web API write/subscribe/read-all commands now forwarded through IPC to workers
- Device dict merging across all worker processes for unified API responses

### Notes
- Phase 2 (Worker Isolation): Each enabled interface now runs in its own process
  with a dedicated bacpypes3 Application — no UDP port 47808 conflicts
- Phase 3 will add device merging/namespacing and a unified subscription view

## [0.1.0] - 2026-09-15

### Added
- Multi-interface configuration schema supporting multiple BACnet/IP interfaces
- Automatic network interface discovery using `ifaddr` with HA Supervisor API fallback
- Per-interface BACnet device identity (unique objectIdentifier/objectName)
- Per-interface device polling and CoV subscription configuration
- Interface validation ensuring unique device identifiers across interfaces
- New `init-interface` s6 oneshot service for add-on initialization
- New `bacnet` s6 longrun service replacing upstream `interface` service
- Translations for all configuration options (English)

### Changed
- Rewrote `main.py` to use interface discovery instead of hardcoded single-interface detection
- Replaced `psutil` interface detection with `ifaddr` library
- Updated `config.yaml` with `interfaces` list schema (replaces single address/objectIdentifier)
- Restructured s6-overlay services for multi-interface architecture

### Notes
- Phase 1 (Foundation): Single-interface operation using the new config schema
- Phase 2 will add process-per-interface worker spawning for simultaneous multi-interface operation
