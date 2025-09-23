import json
import pytest
from unittest.mock import patch, MagicMock
from tedge_modbus.operations import topic_switcher


class DummyContext:
    broker = "localhost"
    port = 1883


@pytest.fixture
def arguments():
    return json.dumps(
        {
            "delivery": {
                "log": [],
                "time": "2025-09-18T12:33:55.628Z",
                "status": "PENDING",
            },
            "agentId": "348474836",
            "creationTime": "2025-09-18T12:33:55.564Z",
            "deviceId": "528472067",
            "id": "8476306",
            "status": "PENDING",
            "c8y_SetRegister": {
                "address": 1,
                "startBit": 0,
                "noBits": 16,
                "ipAddress": "127.0.0.1",
                "value": 4321,
                "register": 3,
            },
            "externalSource": {
                "externalId": "tedge-modbus-test:device:cloudbus",
                "type": "c8y_Serial",
            },
        }
    )


@patch("tedge_modbus.operations.topic_switcher.mqtt.Client")
def test_run_publishes_correct_topic_and_payload(mock_mqtt, arguments):
    mock_client = MagicMock()
    mock_mqtt.return_value = mock_client
    context = DummyContext()
    topic_switcher.run(arguments, context)
    # check broker and port
    mock_client.connect.assert_called_with("localhost", 1883, 60)
    # check topic and payload
    expected_topic = "te/device/cloudbus///cmd/c8y_SetRegister"
    expected_payload = json.dumps(
        {
            "c8y_SetRegister": {
                "address": 1,
                "startBit": 0,
                "noBits": 16,
                "ipAddress": "127.0.0.1",
                "value": 4321,
                "register": 3,
            }
        }
    )
    mock_client.publish.assert_called_with(expected_topic, expected_payload)
