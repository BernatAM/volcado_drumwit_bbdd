import importlib
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.fixture(scope="session")
def etl_modules():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    environment = {
        "SUPABASE_URL": "https://offline.invalid",
        "SUPABASE_KEY": "offline-test-key",
        "MYSQL_HOST": "offline.invalid",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "offline",
        "MYSQL_PASSWORD": "offline",
        "MYSQL_DATABASE": "offline",
        "INCREMENTAL_HOURS": "24",
    }
    # No .env real, cliente remoto ni conexiones al importar los módulos.
    with patch.dict(os.environ, environment), patch("dotenv.load_dotenv", return_value=False), \
            patch("supabase.create_client", return_value=Mock()):
        return importlib.import_module("volcado_clientes"), importlib.import_module("volcado_reservas")


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Las pruebas del ETL no pueden abrir conexiones de red")

    monkeypatch.setattr("socket.socket.connect", forbidden)
    monkeypatch.setattr("socket.socket.connect_ex", forbidden)
