"""
Unit tests for ModeManager and online/offline switching SLAs.
"""

import time
import pytest
from heartbeat.connectivity import ModeManager


class MockCloudClient:
    def __init__(self, initially_healthy: bool = True):
        self.healthy = initially_healthy

    def check_health(self) -> bool:
        return self.healthy


def test_mode_switch_to_offline_and_online():
    mock_client = MockCloudClient(initially_healthy=True)
    mode_mgr = ModeManager(cloud_client=mock_client, failure_threshold_seconds=1.0)

    # Initial probe -> online
    mode = mode_mgr.check_now()
    assert mode == "online"

    # Simulate cloud failure
    mock_client.healthy = False
    time.sleep(1.1)
    mode = mode_mgr.check_now()
    assert mode == "offline"

    # Simulate cloud recovery
    mock_client.healthy = True
    mode = mode_mgr.check_now()
    assert mode == "online"


def test_mode_switch_listener_callback():
    mock_client = MockCloudClient(initially_healthy=False)
    mode_mgr = ModeManager(cloud_client=mock_client, failure_threshold_seconds=0.5)

    recorded_transitions = []
    mode_mgr.add_listener(lambda new_m: recorded_transitions.append(new_m))

    mode_mgr.check_now()  # sets offline
    mock_client.healthy = True
    mode_mgr.check_now()  # transitions to online
    mock_client.healthy = False
    time.sleep(0.6)
    mode_mgr.check_now()  # transitions to offline

    assert "online" in recorded_transitions
    assert "offline" in recorded_transitions
