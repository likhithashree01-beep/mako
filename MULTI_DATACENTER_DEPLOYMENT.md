# Multi-Datacenter Mako Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying and testing Mako across multiple physical datacenters (VMs). It covers everything from VM setup to measuring actual cross-datacenter latency and performance metrics.

---

## Minimum VM Requirements

### **Option 1: Minimal Setup (3 VMs)**
Test basic replication with 1 leader + 2 followers:
- **VM1** (Leader datacenter): `localhost` role
- **VM2** (Follower datacenter 1): `p1` role  
- **VM3** (Follower datacenter 2): `p2` role

### **Option 2: Recommended Setup (4 VMs)**
Full replication with learner for complete metrics:
- **VM1** (Leader datacenter): `localhost` role
- **VM2** (Follower datacenter 1): `p1` role
- **VM3** (Follower datacenter 2): `p2` role
- **VM4** (Learner replica): `learner` role

### VM Specifications (Each VM)
- **CPU**: 4-8 cores
- **RAM**: 8-16 GB
- **Disk**: 50+ GB SSD
- **Network**: Public/Private IP with open ports
- **OS**: Ubuntu 20.04+ or similar Linux

---

## Step-by-Step Setup

### Step 1: Provision VMs in Different Regions

Deploy VMs in geographically distributed regions (examples):
- **VM1**: AWS us-east-1 (N. Virginia)
- **VM2**: AWS eu-west-1 (Ireland)
- **VM3**: AWS ap-southeast-1 (Singapore)
- **VM4**: AWS us-west-2 (Oregon) - Optional

> **📝 Note**: You can use any cloud provider (AWS, GCP, Azure) or physical machines. The key is geographic distribution.

### Step 2: Network Configuration

#### 2.1 Configure Security Groups/Firewall

Open the following ports on **all VMs**:

| Port Range | Purpose | Protocol |
|------------|---------|----------|
| `31000-31999` | Shard communication (localhost) | TCP |
| `32000-32999` | Shard communication (p1) | TCP |
| `33000-33999` | Shard communication (p2) | TCP |
| `34000-34999` | Shard communication (learner) | TCP |
| `6001-6003` | Memory control ports | TCP |
| `22` | SSH access | TCP |

#### 2.2 Test Network Connectivity

From each VM, test connectivity to all other VMs:
```bash
# From VM1, test to VM2, VM3, VM4
ping -c 4 <VM2_IP>
ping -c 4 <VM3_IP>
ping -c 4 <VM4_IP>

# Measure baseline latency
ping -c 100 <VM2_IP> | tail -1
```

Record baseline network latencies between VMs - this is your real WAN latency!

### Step 3: Install Mako on All VMs

On **each VM**, run:

```bash
# Clone the repository
cd ~
git clone <your-mako-repo-url>
cd mako

# Install dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y build-essential cmake git \
    python3 python3-pip libyaml-cpp-dev libboost-all-dev \
    libaio-dev libgflags-dev

# Build Mako
make build

# Verify the build
ls build/dbtest  # Should exist
```

### Step 4: Modify Configuration Files

#### 4.1 Gather VM IP Addresses

Create a note with your VM IPs:
```
VM1 (localhost): 54.123.45.67
VM2 (p1):        52.234.56.78
VM3 (p2):        13.45.67.89
VM4 (learner):   18.56.78.90
```

#### 4.2 Create Multi-DC Configuration

On **VM1**, create a new config file:

```bash
cd ~/mako/src/mako/config
cp local-shards1-warehouses6.yml multidc-shards1-warehouses6.yml
```

Edit `multidc-shards1-warehouses6.yml` with your actual IPs:

```yaml
# Multi-datacenter configuration for 1 shard with replication
shards: 1
replicas: 3  # or 4 if using learner
warehouses: 6

localhost:
  - name: shard0
    index: 0
    ip:   54.123.45.67      # VM1 public IP
    port: 31000

p1:
  - name: shard0
    index: 0
    ip:   52.234.56.78      # VM2 public IP
    port: 32000

p2:
  - name: shard0
    index: 0
    ip:   13.45.67.89       # VM3 public IP
    port: 33000

learner:
  - name: shard0
    index: 0
    ip:   18.56.78.90       # VM4 public IP
    port: 34000

memlocalhost: 6001
memp1: 6002
memp2: 6003
memlearner: 6004
```

#### 4.3 Disable WAN Simulation

Since you're testing with real network latency, disable simulated delays:

```bash
cd ~/mako
# Edit the constants file
nano src/deptran/constants.h
```

Comment out the `SIMULATE_WAN` line:
```cpp
// #define SIMULATE_WAN
```

Rebuild on all VMs:
```bash
make rebuild
```

#### 4.4 Copy Configuration to All VMs

From VM1, copy the config to other VMs:
```bash
scp ~/mako/src/mako/config/multidc-shards1-warehouses6.yml user@VM2:~/mako/src/mako/config/
scp ~/mako/src/mako/config/multidc-shards1-warehouses6.yml user@VM3:~/mako/src/mako/config/
scp ~/mako/src/mako/config/multidc-shards1-warehouses6.yml user@VM4:~/mako/src/mako/config/
```

### Step 5: Create Multi-DC Test Script

On **VM1**, create a test orchestration script:

```bash
cd ~/mako
nano run_multidc_test.sh
```

Add the following content:

```bash
#!/bin/bash

# Multi-datacenter test orchestration script
# Run this on VM1 to start all instances via SSH

# Configuration
VM1_IP="54.123.45.67"
VM2_IP="52.234.56.78"
VM3_IP="13.45.67.89"
VM4_IP="18.56.78.90"
SSH_USER="ubuntu"  # Adjust to your SSH user
THREADS=6
TEST_DURATION=60  # seconds

echo "========================================="
echo "Multi-Datacenter Mako Test"
echo "========================================="

# Clean up old processes
echo "Cleaning up old processes..."
ssh ${SSH_USER}@${VM1_IP} "pkill -9 -f dbtest" 2>/dev/null || true
ssh ${SSH_USER}@${VM2_IP} "pkill -9 -f dbtest" 2>/dev/null || true
ssh ${SSH_USER}@${VM3_IP} "pkill -9 -f dbtest" 2>/dev/null || true
ssh ${SSH_USER}@${VM4_IP} "pkill -9 -f dbtest" 2>/dev/null || true

sleep 2

# Start learner (VM4)
echo "Starting learner on VM4..."
ssh ${SSH_USER}@${VM4_IP} "cd ~/mako && nohup bash bash/shard.sh 1 0 ${THREADS} learner 0 1 > multidc_learner.log 2>&1 &"
sleep 1

# Start follower p2 (VM3)
echo "Starting follower p2 on VM3..."
ssh ${SSH_USER}@${VM3_IP} "cd ~/mako && nohup bash bash/shard.sh 1 0 ${THREADS} p2 0 1 > multidc_p2.log 2>&1 &"
sleep 1

# Start follower p1 (VM2)
echo "Starting follower p1 on VM2..."
ssh ${SSH_USER}@${VM2_IP} "cd ~/mako && nohup bash bash/shard.sh 1 0 ${THREADS} p1 0 1 > multidc_p1.log 2>&1 &"
sleep 2

# Start leader (VM1)
echo "Starting leader on VM1..."
ssh ${SSH_USER}@${VM1_IP} "cd ~/mako && nohup bash bash/shard.sh 1 0 ${THREADS} localhost 0 1 > multidc_leader.log 2>&1 &"

echo ""
echo "All instances started. Running for ${TEST_DURATION} seconds..."
sleep ${TEST_DURATION}

# Stop all processes
echo ""
echo "Stopping all instances..."
ssh ${SSH_USER}@${VM1_IP} "pkill -9 -f 'dbtest.*shard-index 0'" 2>/dev/null || true
ssh ${SSH_USER}@${VM2_IP} "pkill -9 -f 'dbtest.*shard-index 0'" 2>/dev/null || true
ssh ${SSH_USER}@${VM3_IP} "pkill -9 -f 'dbtest.*shard-index 0'" 2>/dev/null || true
ssh ${SSH_USER}@${VM4_IP} "pkill -9 -f 'dbtest.*shard-index 0'" 2>/dev/null || true

echo ""
echo "========================================="
echo "Test completed! Collecting results..."
echo "========================================="

# Collect logs from all VMs
echo "Fetching logs..."
scp ${SSH_USER}@${VM1_IP}:~/mako/multidc_leader.log ./results/
scp ${SSH_USER}@${VM2_IP}:~/mako/multidc_p1.log ./results/
scp ${SSH_USER}@${VM3_IP}:~/mako/multidc_p2.log ./results/
scp ${SSH_USER}@${VM4_IP}:~/mako/multidc_learner.log ./results/

echo ""
echo "Logs saved to ./results/"
echo "Run analysis: grep -h 'agg_persist_throughput\\|Wan_wait\\|replay_batch' ./results/*.log"
```

Make it executable:
```bash
chmod +x run_multidc_test.sh
mkdir -p results
```

### Step 6: Setup SSH Key Authentication (Optional but Recommended)

On VM1, generate SSH keys and copy to all VMs for passwordless access:

```bash
# Generate SSH key (if not already done)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

# Copy to all VMs
ssh-copy-id user@<VM2_IP>
ssh-copy-id user@<VM3_IP>
ssh-copy-id user@<VM4_IP>
```

### Step 7: Run the Multi-Datacenter Test

On **VM1**:

```bash
cd ~/mako
./run_multidc_test.sh
```

### Step 8: Analyze Results

After the test completes, examine the metrics:

```bash
cd ~/mako/results

# Check aggregate persistence throughput
grep "agg_persist_throughput" *.log

# Check WAN latency (real cross-datacenter delays)
grep "Wan_wait" *.log | head -20

# Check follower replay metrics
grep "replay_batch" *.log | tail -10

# Check commit rates
grep "RECORDING_RESULT" *.log
```

---

## Measuring Real Network Latency

### Before Test: Measure Baseline Network Latency

From VM1:
```bash
# Measure to VM2 (p1)
ping -c 100 <VM2_IP> | tail -1
# Example output: rtt min/avg/max/mdev = 75.123/76.456/78.901/1.234 ms

# Measure to VM3 (p2)
ping -c 100 <VM3_IP> | tail -1

# Measure to VM4 (learner)
ping -c 100 <VM4_IP> | tail -1
```

### During Test: Monitor Replication Latency

The `Wan_wait` metric in logs shows **actual replication delay** (in microseconds):

```bash
# Extract Wan_wait values from leader logs
grep "Wan_wait:" results/multidc_leader.log | awk '{print $NF}' | sort -n | tail -20
```

Compare with your baseline ping measurements.

---

## Key Metrics to Track

| Metric | Location | Meaning |
|--------|----------|---------|
| `agg_persist_throughput` | Leader log | Total ops/sec across all replicas |
| `Wan_wait` | Leader log | Cross-datacenter replication latency (μs) |
| `replay_batch` | Follower logs | Number of batches replayed by followers |
| `TPS` | All logs | Transactions per second per instance |
| `LATENCY` | All logs | Transaction latency percentiles |

---

## Troubleshooting

### Issue: "Connection refused" errors

**Solution**: Verify firewall rules and security groups allow traffic on ports 31000-34999.

```bash
# Test port connectivity from VM1 to VM2
nc -zv <VM2_IP> 32000
```

### Issue: High packet loss between VMs

**Solution**: Check network stability:
```bash
mtr -c 100 <VM_IP>
```

### Issue: Processes crash immediately

**Solution**: Check logs for specific errors:
```bash
tail -50 multidc_leader.log
```

### Issue: "Cannot find config file" error

**Solution**: Ensure the Paxos config files exist:
```bash
ls ~/mako/config/1leader_2followers/paxos6_shardidx0.yml
```

If missing, they might be gitignored. You'll need to check the repository or generate them.

---

## Advanced: Testing Datacenter Failure

To test datacenter failover, you can kill the leader during the test:

On VM1 (while test is running):
```bash
# Wait 30 seconds, then kill leader
sleep 30
pkill -9 -f "dbtest.*localhost"
```

Monitor the followers (p1/p2) logs to see failover behavior.

---

## Next Steps

1. **Scale up**: Increase threads (warehouses) to stress test
2. **Multi-shard**: Test with 2+ shards across datacenters
3. **Benchmarking**: Run longer tests (5-10 minutes) for stable metrics
4. **Tuning**: Adjust Paxos parameters in `config/occ_paxos.yml`

---

## Summary Checklist

- [ ] Provision 3-4 VMs in different geographic regions
- [ ] Open required ports (31000-34999, 6001-6004)
- [ ] Install Mako on all VMs
- [ ] Disable `SIMULATE_WAN` and rebuild
- [ ] Create multi-DC config with real VM IPs
- [ ] Setup SSH key authentication
- [ ] Run orchestration script from VM1
- [ ] Collect and analyze logs
- [ ] Compare measured `Wan_wait` with baseline network latency

---

**You're now testing Mako with real cross-datacenter network conditions!** 🚀
