import pytest
from unittest.mock import patch, MagicMock
from tedge_modbus.operations import update_coil
from pathlib import Path


class DummyContext:
    base_config = {"modbus": {"loglevel": "DEBUG"}}
    config_dir = Path("/tmp")


@pytest.fixture
def arguments():
    return '{"ipAddress": "127.0.0.1", "address": 1, "coil": 2, "value": 1}'


@patch("tedge_modbus.operations.update_coil.prepare_client")
def test_run_success(mock_prepare_client, arguments):
    mock_client = MagicMock()
    mock_write_resp = MagicMock()
    mock_write_resp.isError.return_value = False
    mock_client.write_coil.return_value = mock_write_resp
    mock_prepare_client.return_value = mock_client
    context = DummyContext()
    update_coil.run(arguments, context)
    mock_client.write_coil.assert_called()
