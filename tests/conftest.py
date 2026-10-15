"""
Pytest configuration and common fixtures.
"""

import os
import shutil
import tempfile
import pytest

from heartbeat.config import PipelineConfig
from heartbeat.storage import DeviceStorage
from cloud.storage import CloudStorage


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="heartbeat_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def device_db_path(temp_dir):
    return os.path.join(temp_dir, "test_device.db")


@pytest.fixture
def cloud_db_path(temp_dir):
    return os.path.join(temp_dir, "test_cloud.db")


@pytest.fixture
def device_storage(device_db_path):
    return DeviceStorage(db_path=device_db_path, synchronous="NORMAL")


@pytest.fixture
def cloud_storage(cloud_db_path):
    return CloudStorage(db_url=f"sqlite:///{cloud_db_path}")
