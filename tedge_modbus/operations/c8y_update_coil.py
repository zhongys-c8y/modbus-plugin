#!/usr/bin/env python3
"""Cumulocity IoT Modbus Write Coil Status operation handler"""
import json
import logging
import toml
from paho.mqtt.publish import single as mqtt_publish

from .context import Context
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.exceptions import ConnectionException

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def run(arguments, context: Context):
    """Run c8y_update_coil operation handler"""
    # Expected arguments (JSON):
    # {
    #     "id": < operationId >,
    #     "c8y_SetCoil": {
    #        "ipAddress": < ipaddress or empty >,
    #       "address": < Fieldbusaddress >,
    #       "coil": < coilnumber >,
    #       "value": < 0 | 1 >}
    # }
    # Parse JSON payload
    if isinstance(arguments, str):
        raw = arguments
    else:
        raw = arguments[0] if len(arguments) == 1 else ",".join(arguments)
    try:
        payload = json.loads(raw)
    except Exception as err:
        raise ValueError(f"Invalid JSON payload: {err}") from err

    # Load configs and set log level
    modbus_config = context.base_config
    loglevel = modbus_config["modbus"].get("loglevel") or "INFO"
    logger.setLevel(getattr(logging, loglevel.upper(), logging.INFO))
    logger.info("New c8y_update_coil operation")

    try:
        ops = payload["c8y_SetCoil"]
        ip_address = (ops.get("ipAddress") or "").strip()
        slave_id = int(ops["address"])  # Fieldbus address
        coil_number = int(ops["coil"])   # Coil address
        value_int = int(ops["value"])    # 0 or 1
    except KeyError as err:
        raise ValueError(f"Missing required field: {err}") from err
    except ValueError as err:
        raise ValueError(f"Invalid numeric field: {err}") from err

    # Read device definition to find connection parameters
    devices_path = context.config_dir / "devices.toml"
    target_device = None
    protocol = None
    if ip_address:
        protocol = "TCP"
        target_device = {"protocol": "TCP", "ip": ip_address, "port": 502, "address": slave_id}
    else:
        devices_cfg = toml.load(devices_path)
        devices = devices_cfg.get("device", []) or []
        target_device = next((d for d in devices if d.get("address") == slave_id), None) or \
                        next((d for d in devices if d.get("protocol") == "TCP"), None)
        if target_device is None:
            raise ValueError(f"No suitable device found in {devices_path}")
        protocol = target_device.get("protocol")

    # For RTU, backfill serial settings from base config if missing
    if protocol == "RTU":
        serial_defaults = modbus_config.get("serial") or {}
        for key in ["port", "baudrate", "stopbits", "parity", "databits"]:
            if target_device.get(key) is None and key in serial_defaults:
                target_device[key] = serial_defaults[key]

    # Build Modbus client
    if protocol == "TCP":
        client = ModbusTcpClient(
            host=target_device["ip"],
            port=target_device["port"],
            auto_open=True,
            auto_close=True,
            debug=True,
        )
    elif protocol == "RTU":
        client = ModbusSerialClient(
            port=target_device["port"],
            baudrate=target_device["baudrate"],
            stopbits=target_device["stopbits"],
            parity=target_device["parity"],
            bytesize=target_device["databits"],
        )
    else:
        raise ValueError(
            "Expected protocol to be RTU or TCP. Got "
            + str(protocol)
            + "."
        )

    try:
        coil_value = True if value_int == 1 else False
        result = client.write_coil(address=coil_number, value=coil_value, slave=slave_id)
        if result.isError():
            raise RuntimeError(f"Failed to write coil {coil_number}: {result}")
        logger.info("Wrote %s to coil %d on slave %d", coil_value, coil_number, slave_id)
    except ConnectionException as err:
        logger.error("Connection error while writing to slave %d: %s", slave_id, err)
        raise
    finally:
        try:
            client.close()
        except Exception: 
            pass

