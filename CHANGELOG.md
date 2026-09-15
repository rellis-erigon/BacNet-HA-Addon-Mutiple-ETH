# Changelog

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
