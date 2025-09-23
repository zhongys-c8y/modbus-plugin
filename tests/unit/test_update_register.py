import pytest
from unittest.mock import patch, MagicMock
from tedge_modbus.operations import update_register
from pathlib import Path


class DummyContext:
    base_config = {"modbus": {"loglevel": "DEBUG"}}
    config_dir = Path("/tmp")


@pytest.fixture
def arguments():
    return '{"ipAddress": "127.0.0.1", "address": 1, "register": 3, "startBit": 0, "noBits": 16, "value": 10}'


@patch("tedge_modbus.operations.update_register.prepare_client")
@patch("tedge_modbus.operations.update_register.parse_register_params")
def test_run_success(mock_parse_params, mock_prepare_client, arguments):
    mock_client = MagicMock()
    mock_prepare_client.return_value = mock_client
    mock_parse_params.return_value = {
        "ip_address": "127.0.0.1",
        "slave_id": 1,
        "register": 3,
        "start_bit": 0,
        "num_bits": 16,
        "write_value": 321,
    }
    mock_read_resp = MagicMock()
    mock_read_resp.isError.return_value = False
    mock_read_resp.registers = [0]
    mock_client.read_holding_registers.return_value = mock_read_resp

    mock_write_resp = MagicMock()
    mock_write_resp.isError.return_value = False
    mock_client.write_register.return_value = mock_write_resp

    context = DummyContext()
    update_register.run(arguments, context)
    mock_client.write_register.assert_called()
