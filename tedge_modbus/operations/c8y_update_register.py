#!/usr/bin/env python3
"""Cumulocity IoT Modbus Write register status operation handler"""
import logging

from pymodbus.exceptions import ConnectionException
from .context import Context
from .common import (
    parse_json_arguments,
    prepare_client,
    apply_loglevel,
    close_client_quietly,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def run(arguments: str | list[str], context: Context) -> None:
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
    payload = parse_json_arguments(arguments)

    # Load configs and set log level
    modbus_config = context.base_config
    apply_loglevel(logger, modbus_config)
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
    except (TypeError, ValueError) as err:
        raise ValueError(f"Invalid numeric field: {err}") from err

    # Prepare client (resolve target, backfill defaults, build client)
    client = prepare_client(
        ip_address,
        slave_id,
        context.config_dir / "devices.toml",
        modbus_config,
    )

    # Validate bit-field range
    if start_bit < 0 or num_bits <= 0 or start_bit + num_bits > 16:
        raise ValueError(
            "startBit and noBits must define a range within a 16-bit register"
        )
    max_value = (1 << num_bits) - 1
    if write_value < 0 or write_value > max_value:
        raise ValueError(f"value must be within 0..{max_value} for noBits={num_bits}")

    try:
        # Read current register value
        read_resp = client.read_holding_registers(
            address=register_number, count=1, slave=slave_id
        )
        if read_resp.isError():
            raise RuntimeError(
                f"Failed to read register {register_number}: {read_resp}"
            )
        current_value = read_resp.registers[0] & 0xFFFF

        # Compute masked value
        mask = ((1 << num_bits) - 1) << start_bit
        new_value = (current_value & ~mask) | ((write_value << start_bit) & mask)

        # Write back register
        write_resp = client.write_register(
            address=register_number, value=new_value, slave=slave_id
        )
        if write_resp.isError():
            raise RuntimeError(
                f"Failed to write register {register_number}: {write_resp}"
            )
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
        close_client_quietly(client)
