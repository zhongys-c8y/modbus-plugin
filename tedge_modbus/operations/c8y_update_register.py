#!/usr/bin/env python3
"""Cumulocity IoT Modbus Write register status operation handler"""
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
    """Run c8y_update_register operation handler"""
    # Expected arguments (JSON):
    #     {
    #     "ipAddress": <ip address or empty>,
    #     "address": <Fieldbus address>,
    #     "register": <register number>,
    #     "startBit": <start bit>,
    #     "noBits": <number of bits>,
    #     "value": <register value>
    #   }
    # Parse JSON arguments. Depending on the caller, we may receive the JSON as a single
    # string or a list of comma-split segments. Handle both cases robustly.
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
    logger.info("New c8y_update_register operation")

    # Required fields from JSON
    ip_address = (payload.get("ipAddress") or "").strip()
    try:
        slave_id = int(payload["address"])  # Fieldbus address
        register_number = int(payload["register"])  # Register address
        start_bit = int(payload.get("startBit", 0))
        num_bits = int(payload.get("noBits", 16))
        write_value = int(payload["value"])  # New value for the bit-field
    except KeyError as err:
        raise ValueError(f"Missing required field: {err}") from err
    except ValueError as err:
        raise ValueError(f"Invalid numeric field: {err}") from err

    # Determine connection parameters
    devices_path = context.config_dir / "devices.toml"
    protocol = None
    target_device = None
    if ip_address:
        # Direct TCP connection using provided IP; default port 502
        protocol = "TCP"
        target_device = {"protocol": "TCP", "ip": ip_address, "port": 502, "address": slave_id}
    else:
        # Fallback to devices.toml
        devices_cfg = toml.load(devices_path)
        devices = devices_cfg.get("device", []) or []
        # Prefer a TCP device with matching slave address; otherwise first TCP device
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

    # Validate bit-field range
    if start_bit < 0 or num_bits <= 0 or start_bit + num_bits > 16:
        raise ValueError("startBit and noBits must define a range within a 16-bit register")
    max_value = (1 << num_bits) - 1
    if write_value < 0 or write_value > max_value:
        raise ValueError(f"value must be within 0..{max_value} for noBits={num_bits}")

    try:
        # Read current register value
        read_resp = client.read_holding_registers(address=register_number, count=1, slave=slave_id)
        if read_resp.isError():
            raise RuntimeError(f"Failed to read register {register_number}: {read_resp}")
        current_value = read_resp.registers[0] & 0xFFFF

        # Compute masked value
        mask = ((1 << num_bits) - 1) << start_bit
        new_value = (current_value & ~mask) | ((write_value << start_bit) & mask)

        # Write back register
        write_resp = client.write_register(address=register_number, value=new_value, slave=slave_id)
        if write_resp.isError():
            raise RuntimeError(f"Failed to write register {register_number}: {write_resp}")
        logger.info(
            "Updated register %d (bits %d..%d) from 0x%04X to 0x%04X on slave %d",
            register_number,
            start_bit,
            start_bit + num_bits - 1,
            current_value,
            new_value,
            slave_id,
        )
    except ConnectionException as err:
        logger.error("Connection error while writing to slave %d: %s", slave_id, err)
        raise
    finally:
        try:
            client.close()
        except Exception: 
            pass
