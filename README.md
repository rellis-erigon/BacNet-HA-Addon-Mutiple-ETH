# BACnet/IP Multi-Interface Add-on for Home Assistant

A Home Assistant add-on that discovers and communicates with BACnet devices across **multiple network interfaces simultaneously**. Each Ethernet adapter runs its own BACnet/IP stack in an isolated process, with a unified API that merges device data from all interfaces.

Based on the [Bepacom BACnet/IP Interface](https://github.com/Bepacom-Raalte/bepacom-HA-Addons), re-engineered for multi-network environments.

## Features

- **Multi-interface support** — bind BACnet/IP to multiple Ethernet adapters (eth0, eth1, etc.) at the same time
- **Automatic interface discovery** — detects eligible network adapters via `ifaddr` with HA Supervisor API enrichment
- **Process-per-interface isolation** — each interface runs in its own process with a dedicated `bacpypes3` Application, eliminating UDP port conflicts
- **Unified device view** — devices from all interfaces are merged into a single API and web UI
- **Per-interface configuration** — unique BACnet device identity, CoV subscriptions, polling rates, entity lists, and foreign BBMD settings per interface
- **Interface status API** — real-time visibility into each interface's health, device count, and subscriptions
- **Full BACnet/IP feature set** — Who-Is/I-Am discovery, Change of Value (CoV) subscriptions, property read/write, time synchronization, EDE file import

## Installation

Add this repository URL to your Home Assistant add-on store:

```
https://github.com/rellis-erigon/BacNet-HA-Addon-Mutiple-ETH
```

**Settings > Add-ons > Add-on Store > Menu (top right) > Repositories > Paste URL > Add**

Then install **BACnet/IP Multi-Interface** from the store.

## Configuration

The add-on is configured through the Home Assistant UI. The key configuration block is `interfaces`, a list where each entry represents one network interface:

```yaml
interfaces:
  - name: eth0          # Interface name or "auto"
    enabled: true
    address: auto       # IP/CIDR or "auto" to use current address
    objectIdentifier: 420  # Unique BACnet device instance (must differ per interface)
    objectName: BACnet-HA-eth0
    foreignBBMD: "-"    # BBMD address or "-" to disable
    foreignTTL: 255
    entity_list: []     # HA entities to expose as BACnet objects
    devices_setup:      # Per-device CoV/polling config
      - deviceID: all
        CoV_lifetime: 600
        CoV_list: [all]
        quick_poll_rate: 5
        quick_poll_list: []
        slow_poll_rate: 600
        slow_poll_list: [all]
        resub_on_iam: true
        reread_on_iam: false

  - name: eth1
    enabled: true
    address: auto
    objectIdentifier: 421  # Different from eth0
    objectName: BACnet-HA-eth1
    foreignBBMD: "-"
    foreignTTL: 255
    entity_list: []
    devices_setup:
      - deviceID: all
        CoV_lifetime: 600
        CoV_list: [all]
        quick_poll_rate: 5
        quick_poll_list: []
        slow_poll_rate: 600
        slow_poll_list: [all]
```

### Global options

| Option | Default | Description |
|--------|---------|-------------|
| `defaultPriority` | `15` | BACnet write priority (1-16) |
| `loglevel` | `WARNING` | Log verbosity: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `segmentation` | `segmentedBoth` | BACnet segmentation support |
| `vendorID` | `15` | BACnet vendor identifier |
| `api_accessible` | `false` | Enable external REST API access |

### Important notes

- Each enabled interface **must** have a unique `objectIdentifier`
- `name: auto` uses the first available Ethernet adapter
- `address: auto` uses the IP currently assigned to the interface
- The add-on requires `host_network: true` for BACnet UDP broadcast access

## Architecture

```
Supervisor Process (main.py)
├── FastAPI + Uvicorn (port 7813)
│   ├── /webapp              — Web UI
│   ├── /apiv1/*             — Legacy REST API
│   ├── /apiv2/*             — v2 REST API
│   ├── /apiv2/interfaces/*  — Interface status API
│   └── /ws                  — WebSocket (real-time updates)
├── Data Collector           — Polls workers, merges device dicts
├── Write/Subscribe Forwarders — Routes API commands to workers
│
├── Worker [eth0]            — Isolated process
│   ├── BACnetIOHandler      — bacpypes3 NormalApplication
│   ├── 192.168.1.x:47808   — Bound to eth0
│   └── Device discovery, CoV, polling
│
├── Worker [eth1]            — Isolated process
│   ├── BACnetIOHandler      — bacpypes3 NormalApplication
│   ├── 10.0.0.x:47808      — Bound to eth1
│   └── Device discovery, CoV, polling
│
└── NGINX reverse proxy (:8099 → :7813)
```

## API Endpoints

### Interface Management (new)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/apiv2/interfaces` | List all interfaces with status |
| GET | `/apiv2/interfaces/{name}` | Interface detail with devices and subscriptions |
| GET | `/apiv2/interfaces/{name}/devices` | Devices on a specific interface |

### Device Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/apiv1/json` | All devices and values (merged across interfaces) |
| GET | `/apiv1/{deviceid}` | Single device data |
| GET | `/apiv1/{deviceid}/{objectid}` | Object data |
| POST | `/apiv1/{deviceid}/{objectid}` | Write property |
| GET | `/apiv1/command/whois` | Broadcast Who-Is on all interfaces |
| GET | `/apiv1/command/iam` | Broadcast I-Am on all interfaces |
| GET | `/apiv1/command/readall` | Trigger read on all devices |

### v2 API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/apiv2/{deviceid}/{objectid}/{propertyid}` | Read property |
| POST | `/apiv2/{deviceid}/{objectid}/{propertyid}` | Write property |
| GET | `/apiv2/cov` | All active CoV subscriptions |
| POST | `/apiv2/cov/{deviceid}/{objectid}` | Create CoV subscription |
| DELETE | `/apiv2/cov/{deviceid}/{objectid}` | Remove CoV subscription |
| POST | `/apiv2/services/timesync` | Time synchronization |
| POST | `/apiv2/services/utctimesync` | UTC time synchronization |

## Supported Architectures

- `amd64`
- `aarch64`
- `armv7`
- `i386`

## Dependencies

- [bacpypes3](https://github.com/JoelBender/bacpypes3) <= 0.0.102 — BACnet/IP protocol stack
- [ifaddr](https://github.com/pydron/ifaddr) >= 0.2.0 — Network interface enumeration
- [FastAPI](https://fastapi.tiangolo.com/) — REST API and WebSocket server
- [Pydantic](https://pydantic.dev/) v2 — Data validation
- [SQLiteDict](https://github.com/RaRe-Technologies/sqlitedict) — Device persistence

## License

[Apache License 2.0](LICENSE)

## Credits

- Original add-on by [Bepacom B.V.](https://github.com/Bepacom-Raalte/bepacom-HA-Addons)
- Multi-interface re-engineering by [rellis-erigon](https://github.com/rellis-erigon)
