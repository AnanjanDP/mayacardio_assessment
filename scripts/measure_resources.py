import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import psutil


def measure_pipeline_resources(duration_seconds: int = 60):
    print("=======================================================================")
    print(f"   RESOURCE UTILIZATION PROFILER ({duration_seconds} SECONDS SUSTAINED)     ")
    print("=======================================================================")

    temp_dir = tempfile.mkdtemp(prefix="res_prof_")
    dev_db = os.path.join(temp_dir, "device.db")
    cloud_db = os.path.join(temp_dir, "cloud.db")
    mock_port = 7130
    cloud_port = 8130

    env = os.environ.copy()
    env["PYTHONPATH"] = f"src;{os.path.abspath('.')}"
    env["PIPELINE_DB_PATH"] = dev_db
    env["PIPELINE_MOCK_PORT"] = str(mock_port)
    env["PIPELINE_CLOUD_URL"] = f"http://127.0.0.1:{cloud_port}"
    env["CLOUD_DB_URL"] = f"sqlite:///{cloud_db}"
    env["CLOUD_PORT"] = str(cloud_port)
    env["PIPELINE_POLL_INTERVAL"] = "0.01"

    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock.biosignal_mock", "--mode", "signal", "--port", str(mock_port), "--duration", str(duration_seconds + 30)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    cloud_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "cloud.api:app", "--port", str(cloud_port), "--host", "127.0.0.1", "--log-level", "warning"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)

    pipe_proc = subprocess.Popen(
        [sys.executable, "-m", "heartbeat.cli", "run"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    p_pipe = psutil.Process(pipe_proc.pid)
    cpu_readings = []
    max_rss_mb = 0.0

    t_start = time.time()
    initial_net = psutil.net_io_counters()

    try:
        print(f"Profiling edge pipeline process PID {pipe_proc.pid}...")
        while time.time() - t_start < duration_seconds:
            try:
                cpu = p_pipe.cpu_percent(interval=1.0)
                mem = p_pipe.memory_info().rss / (1024 * 1024)
                cpu_readings.append(cpu)
                if mem > max_rss_mb:
                    max_rss_mb = mem

                elapsed = int(time.time() - t_start)
                sys.stdout.write(f"\rElapsed: {elapsed}/{duration_seconds}s | CPU: {cpu:.1f}% | Peak RSS: {max_rss_mb:.2f} MB")
                sys.stdout.flush()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
    finally:
        pipe_proc.terminate()
        cloud_proc.terminate()
        mock_proc.terminate()
        time.sleep(0.5)

    final_net = psutil.net_io_counters()
    net_sent_mb = (final_net.bytes_sent - initial_net.bytes_sent) / (1024 * 1024)

    db_size_mb = os.path.getsize(dev_db) / (1024 * 1024) if os.path.exists(dev_db) else 0.0
    wal_path = dev_db + "-wal"
    if os.path.exists(wal_path):
        db_size_mb += os.path.getsize(wal_path) / (1024 * 1024)

    avg_cpu = sum(cpu_readings) / len(cpu_readings) if cpu_readings else 0.0

    scale_factor = 3600.0 / duration_seconds
    projected_disk_mb_hr = db_size_mb * scale_factor
    projected_net_mb_hr = net_sent_mb * scale_factor

    print("\n\n-----------------------------------------------------------------------")
    print("                      MEASURED RESOURCE REPORT                         ")
    print("-----------------------------------------------------------------------")
    print(f"Profile Duration:                  {duration_seconds} seconds")
    print(f"Average CPU Utilization:           {avg_cpu:.2f}% (Limit: 400% on 4 cores)")
    print(f"Peak Resident Memory (RSS):        {max_rss_mb:.2f} MB (Limit: 2048 MB)")
    print(f"Actual Disk Written in Test:       {db_size_mb:.2f} MB")
    print(f"Projected Disk Written / Hour:     {projected_disk_mb_hr:.2f} MB / hour")
    print(f"Projected Network Sent / Hour:     {projected_net_mb_hr:.2f} MB / hour")
    print("-----------------------------------------------------------------------\n")

    shutil.rmtree(temp_dir, ignore_errors=True)
    return avg_cpu, max_rss_mb, projected_disk_mb_hr, projected_net_mb_hr


def main():
    parser = argparse.ArgumentParser(description="Profile pipeline resource utilization")
    parser.add_argument("--duration", type=int, default=15, help="Duration in seconds (default: 15s)")
    args = parser.parse_args()
    measure_pipeline_resources(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
