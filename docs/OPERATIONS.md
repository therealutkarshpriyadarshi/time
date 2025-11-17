# Operations Guide

This guide covers deployment, configuration, monitoring, backup/recovery, and operational procedures for the Time-Series Database (TSDB).

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Monitoring & Observability](#monitoring--observability)
- [Backup & Recovery](#backup--recovery)
- [Maintenance](#maintenance)
- [Troubleshooting](#troubleshooting)
- [Security](#security)

## Installation

### From Source

```bash
# Clone repository
git clone https://github.com/therealutkarshpriyadarshi/time.git
cd time

# Build
go build -o tsdb ./cmd/tsdb

# Install
sudo mv tsdb /usr/local/bin/

# Verify
tsdb --version
```

### Using Docker

```bash
# Pull image
docker pull ghcr.io/therealutkarshpriyadarshi/time:latest

# Run
docker run -d \
  --name tsdb \
  -p 8080:8080 \
  -v tsdb-data:/data \
  ghcr.io/therealutkarshpriyadarshi/time:latest \
  start --data-dir=/data --listen=:8080
```

### Using Docker Compose

```bash
# Download docker-compose.yml
curl -O https://raw.githubusercontent.com/therealutkarshpriyadarshi/time/main/docker-compose.yml

# Start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## Configuration

### Command-Line Options

```bash
tsdb start [options]

Options:
  --listen=ADDR           Listen address (default: :8080)
  --data-dir=PATH         Data directory (default: ./data)
  --retention=DURATION    Data retention period (default: 30d)
  --memtable-size=SIZE    MemTable size in bytes (default: 256MB)
  --wal-enabled           Enable Write-Ahead Log (default: true)
  --wal-segment-size=SIZE WAL segment size (default: 128MB)
  --compaction-enabled    Enable compaction (default: true)
  --compaction-interval=D Compaction interval (default: 5m)
  --log-level=LEVEL       Log level: debug, info, warn, error (default: info)
  --log-format=FORMAT     Log format: json, text (default: json)
```

### Configuration File

Create `tsdb.yaml`:

```yaml
server:
  listen: ":8080"
  read_timeout: 30s
  write_timeout: 30s

storage:
  data_dir: "/var/lib/tsdb/data"
  retention: 720h  # 30 days
  memtable_size: 268435456  # 256 MB

wal:
  enabled: true
  dir: "/var/lib/tsdb/wal"
  segment_size: 134217728  # 128 MB
  sync_interval: 1s

compaction:
  enabled: true
  interval: 5m
  concurrency: 2

logging:
  level: info
  format: json
  output: stdout

metrics:
  enabled: true
  path: /metrics
```

Load configuration:

```bash
tsdb start --config=/etc/tsdb/tsdb.yaml
```

### Environment Variables

```bash
export TSDB_LISTEN=:8080
export TSDB_DATA_DIR=/var/lib/tsdb/data
export TSDB_RETENTION=30d
export TSDB_LOG_LEVEL=info

tsdb start
```

## Deployment

### Systemd Service

Create `/etc/systemd/system/tsdb.service`:

```ini
[Unit]
Description=Time-Series Database
After=network.target

[Service]
Type=simple
User=tsdb
Group=tsdb
ExecStart=/usr/local/bin/tsdb start \
  --listen=:8080 \
  --data-dir=/var/lib/tsdb/data \
  --retention=30d \
  --log-level=info
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/tsdb

[Install]
WantedBy=multi-user.target
```

Manage service:

```bash
# Create user
sudo useradd -r -s /bin/false tsdb

# Create directories
sudo mkdir -p /var/lib/tsdb/data
sudo chown -R tsdb:tsdb /var/lib/tsdb

# Enable and start
sudo systemctl enable tsdb
sudo systemctl start tsdb

# Check status
sudo systemctl status tsdb

# View logs
sudo journalctl -u tsdb -f
```

### Kubernetes

Create `tsdb-deployment.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: tsdb

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: tsdb-data
  namespace: tsdb
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tsdb
  namespace: tsdb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tsdb
  template:
    metadata:
      labels:
        app: tsdb
    spec:
      containers:
      - name: tsdb
        image: ghcr.io/therealutkarshpriyadarshi/time:latest
        args:
          - start
          - --data-dir=/data
          - --listen=:8080
          - --retention=30d
        ports:
        - containerPort: 8080
          name: http
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        livenessProbe:
          httpGet:
            path: /-/healthy
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /-/ready
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: tsdb-data

---
apiVersion: v1
kind: Service
metadata:
  name: tsdb
  namespace: tsdb
spec:
  selector:
    app: tsdb
  ports:
  - protocol: TCP
    port: 8080
    targetPort: 8080
  type: ClusterIP
```

Deploy:

```bash
kubectl apply -f tsdb-deployment.yaml

# Check status
kubectl -n tsdb get pods
kubectl -n tsdb logs -f deployment/tsdb
```

## Monitoring & Observability

### Prometheus Metrics

The TSDB exports metrics at `/metrics` endpoint:

```bash
curl http://localhost:8080/metrics
```

**Configure Prometheus** (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: 'tsdb'
    static_configs:
      - targets: ['localhost:8080']
    scrape_interval: 15s
```

### Key Metrics to Monitor

**Write Path:**
```
tsdb_samples_ingested_total          # Total samples written
tsdb_insert_duration_seconds         # Write latency
tsdb_insert_errors_total             # Write failures
```

**Storage:**
```
tsdb_head_series                     # Active series count
tsdb_blocks_total                    # Number of blocks
tsdb_wal_size_bytes                  # WAL size
```

**Query Path:**
```
tsdb_queries_total                   # Query count
tsdb_query_duration_seconds          # Query latency
tsdb_query_errors_total              # Query failures
```

**System:**
```
tsdb_goroutines                      # Goroutine count
tsdb_memory_alloc_bytes              # Memory usage
```

### Logging

Logs are written to stdout/stderr in JSON format by default:

```json
{
  "level": "info",
  "ts": "2025-01-15T10:30:45Z",
  "msg": "starting TSDB server",
  "listen_addr": ":8080",
  "data_dir": "/var/lib/tsdb/data"
}
```

**Change log level:**

```bash
tsdb start --log-level=debug
```

**Parse logs with jq:**

```bash
# Filter error logs
journalctl -u tsdb -f | jq 'select(.level == "error")'

# Watch write operations
docker logs -f tsdb | jq 'select(.operation == "write")'
```

### Debugging Endpoints

**pprof** (available at `/debug/pprof/`):

```bash
# CPU profile (30 seconds)
curl http://localhost:8080/debug/pprof/profile?seconds=30 > cpu.prof

# Heap profile
curl http://localhost:8080/debug/pprof/heap > heap.prof

# Goroutines
curl http://localhost:8080/debug/pprof/goroutine?debug=1

# Analyze
go tool pprof cpu.prof
```

**Health checks:**

```bash
# Liveness (is server running?)
curl http://localhost:8080/-/healthy

# Readiness (is server ready to serve traffic?)
curl http://localhost:8080/-/ready
```

## Backup & Recovery

### Backup Strategies

#### 1. File System Snapshot

```bash
# Stop writes or enable read-only mode
curl -X POST http://localhost:8080/api/v1/admin/readonly

# Snapshot data directory
rsync -av /var/lib/tsdb/data/ /backup/tsdb-$(date +%Y%m%d)/

# Or use filesystem snapshots (LVM, ZFS, etc.)
lvcreate --snapshot --size 10G --name tsdb-snap /dev/vg0/tsdb

# Resume writes
curl -X POST http://localhost:8080/api/v1/admin/writable
```

#### 2. Continuous Backup (Incremental)

```bash
# Backup script
#!/bin/bash
BACKUP_DIR=/backup/tsdb/$(date +%Y%m%d-%H%M%S)
DATA_DIR=/var/lib/tsdb/data

# Sync blocks (immutable)
rsync -av --link-dest=/backup/tsdb/latest \
  $DATA_DIR/blocks/ $BACKUP_DIR/blocks/

# Copy WAL (always changing)
rsync -av $DATA_DIR/wal/ $BACKUP_DIR/wal/

# Update latest symlink
ln -sfn $BACKUP_DIR /backup/tsdb/latest
```

Schedule with cron:

```bash
# Backup every 6 hours
0 */6 * * * /usr/local/bin/tsdb-backup.sh
```

#### 3. Cloud Backup

```bash
# AWS S3
aws s3 sync /var/lib/tsdb/data/ s3://my-bucket/tsdb-backup/

# Google Cloud Storage
gsutil -m rsync -r /var/lib/tsdb/data/ gs://my-bucket/tsdb-backup/
```

### Recovery Procedures

#### Restore from Backup

```bash
# Stop TSDB
sudo systemctl stop tsdb

# Clear current data
rm -rf /var/lib/tsdb/data/*

# Restore from backup
rsync -av /backup/tsdb-20250115/ /var/lib/tsdb/data/

# Fix permissions
sudo chown -R tsdb:tsdb /var/lib/tsdb

# Start TSDB
sudo systemctl start tsdb

# Verify
curl http://localhost:8080/-/healthy
```

#### WAL Replay

If the process crashes, WAL is automatically replayed on restart:

```bash
# Logs will show:
# "replaying WAL" entries=1234 duration_ms=567
```

#### Disaster Recovery

```bash
# 1. Provision new server
# 2. Install TSDB
# 3. Restore data from backup
# 4. Verify data integrity
curl http://localhost:8080/api/v1/status/tsdb

# 5. Update DNS/load balancer
# 6. Resume traffic
```

## Maintenance

### Routine Tasks

#### 1. Monitor Disk Usage

```bash
# Check data directory size
du -sh /var/lib/tsdb/data

# Check WAL size
du -sh /var/lib/tsdb/data/wal

# Set up alerts
if [ $(du -s /var/lib/tsdb/data | cut -f1) -gt 100000000 ]; then
  echo "WARNING: TSDB data directory >100GB"
fi
```

#### 2. Adjust Retention

```bash
# Update retention via API
curl -X POST http://localhost:8080/api/v1/admin/retention \
  -d '{"retention": "60d"}'

# Or restart with new retention
tsdb start --retention=60d
```

#### 3. Manual Compaction

```bash
# Trigger compaction via API
curl -X POST http://localhost:8080/api/v1/admin/compact

# Or use CLI
tsdb compact --data-dir=/var/lib/tsdb/data
```

#### 4. Check Database Health

```bash
# Get status
curl http://localhost:8080/api/v1/status/tsdb

# Inspect specific block
tsdb inspect /var/lib/tsdb/data/blocks/01HQXXX
```

### Upgrading

#### Rolling Upgrade (Zero Downtime)

For high availability setups with multiple replicas:

```bash
# 1. Upgrade one instance
docker pull ghcr.io/therealutkarshpriyadarshi/time:latest
docker stop tsdb-1
docker rm tsdb-1
docker run -d --name tsdb-1 ... ghcr.io/therealutkarshpriyadarshi/time:latest

# 2. Wait for health check
curl http://tsdb-1:8080/-/ready

# 3. Repeat for other instances
```

#### In-Place Upgrade

```bash
# 1. Backup data
/usr/local/bin/tsdb-backup.sh

# 2. Stop service
sudo systemctl stop tsdb

# 3. Download new binary
wget https://github.com/therealutkarshpriyadarshi/time/releases/download/v1.1.0/tsdb
sudo mv tsdb /usr/local/bin/
sudo chmod +x /usr/local/bin/tsdb

# 4. Start service
sudo systemctl start tsdb

# 5. Verify
tsdb --version
curl http://localhost:8080/-/healthy
```

## Troubleshooting

### Common Issues

#### 1. High Memory Usage

**Symptoms:**
```
OOM kills, tsdb_memory_alloc_bytes high
```

**Solutions:**
```bash
# Reduce MemTable size
tsdb start --memtable-size=128MB

# Check for goroutine leaks
curl http://localhost:8080/debug/pprof/goroutine?debug=1

# Analyze memory profile
curl http://localhost:8080/debug/pprof/heap > heap.prof
go tool pprof heap.prof
```

#### 2. Slow Writes

**Symptoms:**
```
tsdb_insert_duration_seconds p99 >100ms
```

**Solutions:**
```bash
# Check WAL disk I/O
iostat -x 1

# Increase MemTable size
tsdb start --memtable-size=512MB

# Disable WAL (if durability not required)
tsdb start --wal-enabled=false
```

#### 3. Disk Full

**Symptoms:**
```
write errors, "no space left on device"
```

**Solutions:**
```bash
# Reduce retention
curl -X POST http://localhost:8080/api/v1/admin/retention -d '{"retention": "7d"}'

# Manually delete old blocks
find /var/lib/tsdb/data/blocks -type d -mtime +30 -exec rm -rf {} \;

# Add more disk space
# Resize volume, extend filesystem
```

#### 4. Crash/Restart Loop

**Symptoms:**
```
systemctl status tsdb shows "activating/failed"
```

**Solutions:**
```bash
# Check logs
journalctl -u tsdb -n 100

# Check WAL corruption
tsdb inspect-wal /var/lib/tsdb/data/wal

# If corrupted, recover from backup
# Or delete WAL (data loss!)
rm -rf /var/lib/tsdb/data/wal/*
```

### Debug Mode

```bash
# Enable debug logging
tsdb start --log-level=debug

# Watch all operations
journalctl -u tsdb -f | jq 'select(.level == "debug")'
```

### Performance Analysis

```bash
# Live CPU profile
go tool pprof http://localhost:8080/debug/pprof/profile

# Live memory profile
go tool pprof http://localhost:8080/debug/pprof/heap

# Trace execution
curl http://localhost:8080/debug/pprof/trace?seconds=10 > trace.out
go tool trace trace.out
```

## Security

### Network Security

```bash
# Bind to localhost only (internal only)
tsdb start --listen=127.0.0.1:8080

# Use TLS (requires certificate)
tsdb start --listen=:8443 --tls-cert=/path/to/cert.pem --tls-key=/path/to/key.pem
```

### Authentication (Future Enhancement)

Currently, the TSDB does not include built-in authentication. Use a reverse proxy:

```nginx
# nginx configuration
location /api/ {
    auth_basic "TSDB API";
    auth_basic_user_file /etc/nginx/.htpasswd;
    proxy_pass http://localhost:8080/api/;
}
```

### File Permissions

```bash
# Secure data directory
sudo chown -R tsdb:tsdb /var/lib/tsdb
sudo chmod 750 /var/lib/tsdb
sudo chmod 640 /var/lib/tsdb/data/*
```

### Firewall

```bash
# Allow only from specific IPs
sudo ufw allow from 10.0.0.0/8 to any port 8080

# Or use iptables
sudo iptables -A INPUT -p tcp --dport 8080 -s 10.0.0.0/8 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 8080 -j DROP
```

## Conclusion

This operations guide covers the essential procedures for deploying, configuring, monitoring, and maintaining the TSDB in production environments. For additional assistance, consult the [PERFORMANCE.md](PERFORMANCE.md) guide for tuning and optimization, or open an issue on GitHub.
