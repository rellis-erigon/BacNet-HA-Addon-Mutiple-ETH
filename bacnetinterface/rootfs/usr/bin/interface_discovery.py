"""Network interface discovery for BACnet multi-interface add-on."""

import json
import logging
import os
import socket
from typing import Optional

import ifaddr
import requests

LOGGER = logging.getLogger("asyncio")

ELIGIBLE_PREFIXES = ("eth", "enp", "ens", "eno", "end")
EXCLUDED_PREFIXES = ("lo", "docker", "veth", "br-", "hassio", "tun", "tap", "vir")


def get_eligible_interfaces() -> list[dict]:
    """Return network interfaces suitable for BACnet/IP binding.

    Each returned dict contains:
      - name: interface name (e.g. "eth0")
      - ip: IPv4 address string
      - prefix: CIDR prefix length (int)
      - cidr: full "ip/prefix" string
    """
    eligible = []
    seen_names = set()

    for adapter in ifaddr.get_adapters():
        name = adapter.name
        if name in seen_names:
            continue

        if not any(name.startswith(p) for p in ELIGIBLE_PREFIXES):
            continue
        if any(name.startswith(x) for x in EXCLUDED_PREFIXES):
            continue

        ipv4_addrs = [ip for ip in adapter.ips if isinstance(ip.ip, str)]
        if not ipv4_addrs:
            continue

        ip = ipv4_addrs[0]
        seen_names.add(name)
        eligible.append({
            "name": name,
            "ip": ip.ip,
            "prefix": ip.network_prefix,
            "cidr": f"{ip.ip}/{ip.network_prefix}",
        })

    return eligible


def get_supervisor_interfaces() -> Optional[list[dict]]:
    """Query the HA Supervisor API for network interface info.

    Returns a list of interface dicts from the Supervisor, or None if
    the API is unavailable.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        LOGGER.debug("No SUPERVISOR_TOKEN, skipping Supervisor API query")
        return None

    try:
        resp = requests.get(
            "http://supervisor/network/info",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", {}).get("interfaces", [])
    except Exception as e:
        LOGGER.warning(f"Supervisor API network query failed: {e}")

    return None


def resolve_interface_config(interface_cfg: dict, available: list[dict]) -> Optional[dict]:
    """Resolve an interface config entry to a concrete interface.

    Args:
        interface_cfg: One entry from the 'interfaces' config list.
        available: List from get_eligible_interfaces().

    Returns:
        A dict with 'name', 'ip', 'prefix', 'cidr' for the resolved interface,
        or None if it can't be resolved.
    """
    if not interface_cfg.get("enabled", False):
        return None

    cfg_name = interface_cfg.get("name", "auto")
    cfg_address = interface_cfg.get("address", "auto")

    if cfg_name == "auto":
        if not available:
            LOGGER.warning("No eligible interfaces found for 'auto' config")
            return None
        matched = available[0]
    else:
        matched = next((i for i in available if i["name"] == cfg_name), None)
        if not matched:
            LOGGER.warning(f"Configured interface '{cfg_name}' not found among available: {[i['name'] for i in available]}")
            return None

    if cfg_address != "auto":
        addr = cfg_address
        if "/" not in addr:
            addr = f"{addr}/24"
        parts = addr.split("/")
        return {
            "name": matched["name"],
            "ip": parts[0],
            "prefix": int(parts[1]),
            "cidr": addr,
        }

    return matched


def validate_unique_identifiers(interfaces_cfg: list[dict]) -> list[str]:
    """Check that all enabled interfaces have unique objectIdentifiers.

    Returns a list of error messages (empty if valid).
    """
    errors = []
    enabled = [i for i in interfaces_cfg if i.get("enabled", False)]

    ids_seen = {}
    for iface in enabled:
        obj_id = iface.get("objectIdentifier")
        name = iface.get("name", "unknown")
        if obj_id in ids_seen:
            errors.append(
                f"objectIdentifier {obj_id} is used by both "
                f"'{ids_seen[obj_id]}' and '{name}' — must be unique per interface"
            )
        else:
            ids_seen[obj_id] = name

    return errors


def discover_and_resolve(options: dict) -> list[dict]:
    """Main entry point: discover interfaces and resolve config.

    Args:
        options: The full add-on options dict (from /data/options.json).

    Returns:
        List of resolved interface configs, each containing:
          - name, ip, prefix, cidr (from discovery)
          - objectIdentifier, objectName (from config)
          - foreignBBMD, foreignTTL (from config)
          - entity_list (from config)
          - devices_setup (from config)
    """
    available = get_eligible_interfaces()
    LOGGER.info(f"Discovered interfaces: {[i['name'] + '=' + i['cidr'] for i in available]}")

    sup_interfaces = get_supervisor_interfaces()
    if sup_interfaces:
        sup_names = [i.get("interface", "") for i in sup_interfaces if i.get("connected")]
        LOGGER.info(f"Supervisor connected interfaces: {sup_names}")

    interfaces_cfg = options.get("interfaces", [])

    validation_errors = validate_unique_identifiers(interfaces_cfg)
    if validation_errors:
        for err in validation_errors:
            LOGGER.error(err)
        raise ValueError(f"Interface configuration invalid: {'; '.join(validation_errors)}")

    resolved = []
    for iface_cfg in interfaces_cfg:
        iface = resolve_interface_config(iface_cfg, available)
        if iface is None:
            continue

        resolved.append({
            **iface,
            "objectIdentifier": iface_cfg.get("objectIdentifier", 420),
            "objectName": iface_cfg.get("objectName", "BACnet-HA"),
            "foreignBBMD": iface_cfg.get("foreignBBMD", "-"),
            "foreignTTL": iface_cfg.get("foreignTTL", 255),
            "entity_list": iface_cfg.get("entity_list", []),
            "devices_setup": iface_cfg.get("devices_setup", []),
        })

    if not resolved:
        LOGGER.warning("No interfaces resolved — falling back to first available")
        if available:
            default_cfg = interfaces_cfg[0] if interfaces_cfg else {}
            resolved.append({
                **available[0],
                "objectIdentifier": default_cfg.get("objectIdentifier", 420),
                "objectName": default_cfg.get("objectName", "BACnet-HA"),
                "foreignBBMD": default_cfg.get("foreignBBMD", "-"),
                "foreignTTL": default_cfg.get("foreignTTL", 255),
                "entity_list": default_cfg.get("entity_list", []),
                "devices_setup": default_cfg.get("devices_setup", []),
            })

    LOGGER.info(f"Resolved {len(resolved)} interface(s): {[r['name'] + '=' + r['cidr'] for r in resolved]}")
    return resolved
