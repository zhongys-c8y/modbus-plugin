"""Publish Modbus operation payload to tedge MQTT topic."""

import json
import logging
import paho.mqtt.client as mqtt
from .context import Context
from .common import parse_json_arguments

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def run(arguments, context: Context):  # pylint: disable=unused-argument
    """Expected arguments (JSON):
    {"delivery":{"log":[],"time":"2025-09-18T12:33:55.628Z","status":"PENDING"},
    "agentId":"348474836","creationTime":"2025-09-18T12:33:55.564Z","deviceId":"528472067",
    "id":"8476306","status":"PENDING","c8y_SetRegister":{"address":1,"startBit":0,"noBits":16,
    "ipAddress":"127.0.0.1","value":4321,"register":3},
    "externalSource":{"externalId":"tedge-modbus-test:device:cloudbus","type":"c8y_Serial"}}
    """

    try:
        arguments = parse_json_arguments(arguments)
    except Exception as e:
        logger.error("Failed to parse arguments as JSON: %s", e)
        return

    # Extract operation name and device externalId
    operation_key = None
    for key in arguments:
        if key.startswith("c8y_"):
            operation_key = key
            break
    if not operation_key:
        logger.error("No c8y_ operation found in arguments")
        return

    external_id = arguments.get("externalSource", {}).get("externalId", "unknown")
    # Extract device part from externalId (e.g., tedge-modbus-test:device:cloudbus)
    # Use the part after the last colon
    device_part = external_id.split(":")[-1] if ":" in external_id else external_id
    target_topic = f"te/device/{device_part}///cmd/{operation_key}"

    payload = json.dumps({operation_key: arguments[operation_key]})

    ctx = Context()
    broker = ctx.broker
    port = ctx.port

    client = mqtt.Client()
    client.connect(broker, port, 60)
    client.loop_start()
    client.publish(target_topic, payload)
    client.loop_stop()
    client.disconnect()
    logger.info("Published payload to %s via broker %s:%s", target_topic, broker, port)
