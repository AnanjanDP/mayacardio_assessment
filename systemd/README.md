# Raspberry Pi OS systemd Service Installation

These service units ensure headless, unattended 24/7 reliability on ARM64 Raspberry Pi boards. They handle automatic startup on boot, power-loss reboot recovery, and instant respawn if killed by OOM or fault injection.

## 1. Copy Service Units to System Directory

```bash
sudo cp systemd/biosignal-mock.service /etc/systemd/system/
sudo cp systemd/heartbeat-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 2. Enable and Start Services

```bash
# Enable to start on boot
sudo systemctl enable biosignal-mock.service
sudo systemctl enable heartbeat-pipeline.service

# Start immediately
sudo systemctl start biosignal-mock.service
sudo systemctl start heartbeat-pipeline.service
```

## 3. Monitor Logs and Status

```bash
# Check service status
sudo systemctl status heartbeat-pipeline.service

# Live tail system logs
journalctl -u heartbeat-pipeline -f

# Check pipeline health via CLI
./pipeline status
```

## 4. Resource Constraints Enforced
- `MemoryMax=512M`: Prevents runaway memory pressure from impacting the kernel.
- `CPUQuota=150%`: Limits CPU consumption to 1.5 cores max, well within the 4-core Raspberry Pi 4 envelope.
- `Restart=always` with `RestartSec=2s`: Guarantees seamless recovery from process crashes or power-pull reboots.
