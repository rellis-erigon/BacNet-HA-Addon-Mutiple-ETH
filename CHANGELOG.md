# Changelog

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
