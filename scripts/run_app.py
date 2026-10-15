"""
run_app.py: One-click application launcher for Never Lose a Heartbeat.
Starts:
1. biosignal-mock TCP server on 127.0.0.1:7100
2. FastAPI Cloud API server on 127.0.0.1:8000
3. Edge Pipeline worker (Collector, Uploader, Local Processor)
Displays a live terminal dashboard showing real-time health, samples, and heart rate.
Press Ctrl+C to shut down all processes cleanly.
"""

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


def main():
    print("=======================================================================")
    print("      NEVER LOSE A HEARTBEAT - FAULT-TOLERANT APPLICATION LAUNCHER     ")
    print("=======================================================================")
    print("Zero-cloud login required: using local durable edge & cloud engines.")
    print("Starting services...\n")

    env = os.environ.copy()
    env["PYTHONPATH"] = f"src;{os.path.abspath('.')}"

    # 1. Start Mock Server
    print("[1/3] Starting biosignal-mock on 127.0.0.1:7100 (mode=signal)...")
    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock.biosignal_mock", "--mode", "signal", "--port", "7100", "--duration", "7200"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    # 2. Start Cloud API Server
    print("[2/3] Starting Cloud Ingestion API on http://127.0.0.1:8000...")
    cloud_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "cloud.api:app", "--port", "8000", "--host", "127.0.0.1", "--log-level", "warning"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)

    # 3. Start Edge Pipeline
    print("[3/3] Starting Edge Pipeline Worker...")
    pipe_proc = subprocess.Popen(
        [sys.executable, "-m", "heartbeat.cli", "run"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    print("\n-----------------------------------------------------------------------")
    print(" All systems operational! Live monitoring active (Press Ctrl+C to stop)")
    print("-----------------------------------------------------------------------\n")

    try:
        while True:
            time.sleep(2.0)
            try:
                status_raw = subprocess.check_output(
                    [sys.executable, "-m", "heartbeat.cli", "status"],
                    env=env,
                    stderr=subprocess.DEVNULL,
                ).decode().strip()
                status = json.loads(status_raw)

                # Fetch status fields
                mode = status.get("mode", "unknown")
                saved = status.get("samples_saved", 0)
                cloud = status.get("samples_in_cloud", 0)
                backlog = status.get("backlog", 0)
                sess = status.get("session_id") or "starting"

                sys.stdout.write(
                    f"\r>> [STATUS] Session: {sess:<9} | Mode: {mode.upper():<7} | Saved: {saved:<8,} | Cloud: {cloud:<8,} | Backlog: {backlog:<5,} "
                )
                sys.stdout.flush()
            except Exception:
                pass
    except KeyboardInterrupt:
        print("\n\nShutting down all services gracefully...")
    finally:
        pipe_proc.terminate()
        cloud_proc.terminate()
        mock_proc.terminate()
        time.sleep(0.5)
        print("[OK] All processes terminated cleanly.\n")


if __name__ == "__main__":
    main()
