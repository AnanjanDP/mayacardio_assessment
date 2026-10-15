import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import psutil
from heartbeat.connectivity import ModeManager
from heartbeat.storage import DeviceStorage
from heartbeat.uploader import BacklogUploader
from cloud.storage import CloudStorage


def benchmark_storage_insertion(n_samples: int = 100_000):
    temp_dir = tempfile.mkdtemp(prefix="bm_storage_")
    db_path = os.path.join(temp_dir, "bench.db")
    storage = DeviceStorage(db_path=db_path, synchronous="NORMAL")

    print(f"\n--- 1. Storage Persistence Benchmark ({n_samples:,} samples) ---")
    batch_size = 400
    n_batches = n_samples // batch_size

    t0 = time.time()
    for b in range(n_batches):
        idx = b * batch_size
        data_s1 = [[(3 * (idx + j)) % 8388608, (3 * (idx + j) + 1) % 8388608, (3 * (idx + j) + 2) % 8388608] for j in range(batch_size)]
        storage.persist_batch("sess_bm", 1, idx, data_s1, [])

    elapsed = time.time() - t0
    rate = n_samples / elapsed
    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)

    print(f"Persisted {n_samples:,} samples in {elapsed:.2f}s ({rate:,.0f} samples/sec)")
    print(f"Database size: {db_size_mb:.2f} MB ({db_size_mb / (n_samples / 1000):.2f} KB / 1k samples)")

    shutil.rmtree(temp_dir, ignore_errors=True)
    return rate, db_size_mb


def benchmark_outage_recovery(n_samples: int = 100_000):
    print(f"\n--- 2. Outage Recovery Synchronization Benchmark ({n_samples:,} sample test) ---")
    temp_dir = tempfile.mkdtemp(prefix="bm_sync_")
    dev_db = os.path.join(temp_dir, "device.db")
    cloud_db = os.path.join(temp_dir, "cloud.db")

    dev_storage = DeviceStorage(db_path=dev_db, synchronous="NORMAL")
    c_storage = CloudStorage(db_url=f"sqlite:///{cloud_db}")

    print(f"Populating edge backlog of {n_samples:,} samples...")
    batch_size = 400
    for b in range(n_samples // batch_size):
        idx = b * batch_size
        s1_data = [[100, 200, 300] for _ in range(batch_size)]
        dev_storage.persist_batch("outage_sess", 1, idx, s1_data, [])

    class DirectCloudClient:
        def check_health(self):
            return True

        def ingest_batch(self, session_id, samples, loss_records):
            c_storage.ingest_batch(session_id, samples, loss_records)
            return True

    fake_client = DirectCloudClient()
    fake_mode = ModeManager(cloud_client=fake_client)
    fake_mode._mode = "online"

    uploader = BacklogUploader(
        storage=dev_storage,
        cloud_client=fake_client,
        mode_manager=fake_mode,
        batch_size=2000,
    )

    t0 = time.time()
    synced = uploader.sync_all_sessions(drain_backlog=True)
    elapsed = time.time() - t0
    rate = synced / elapsed

    two_hour_samples = 2 * 3600 * 800
    est_recovery_seconds = two_hour_samples / rate
    est_recovery_minutes = est_recovery_seconds / 60.0

    print(f"Synchronized {synced:,} backlog samples in {elapsed:.2f}s ({rate:,.0f} samples/sec)")
    print(f"Projected 2-Hour Outage (5,760,000 samples) Recovery Time: {est_recovery_seconds:.1f}s ({est_recovery_minutes:.2f} minutes)")
    print(f"Target SLA: <= 15 minutes. Result: {'PASS' if est_recovery_minutes <= 15.0 else 'FAIL'}")

    shutil.rmtree(temp_dir, ignore_errors=True)
    return rate, est_recovery_minutes


def benchmark_resource_footprint():
    print("\n--- 3. Resource Footprint Benchmark ---")
    proc = psutil.Process(os.getpid())
    mem_info = proc.memory_info()
    rss_mb = mem_info.rss / (1024 * 1024)
    cpu_percent = proc.cpu_percent(interval=0.5)

    print(f"Current Process RSS Memory: {rss_mb:.2f} MB (Target: < 2048 MB)")
    print(f"Current CPU Usage: {cpu_percent:.1f}% (Target: < 400% on 4 cores)")
    return rss_mb, cpu_percent


def main():
    print("=================================================================")
    print("  NEVER LOSE A HEARTBEAT - SYSTEM PERFORMANCE & RECOVERY REPORT  ")
    print("=================================================================")
    benchmark_storage_insertion(50_000)
    benchmark_outage_recovery(50_000)
    benchmark_resource_footprint()
    print("\n=================================================================\n")


if __name__ == "__main__":
    main()
