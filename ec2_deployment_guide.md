# AWS EC2 Permanent Runner Deployment Guide

This guide provides step-by-step instructions for deploying your **Strategy B Automated Trading Engine** onto an AWS EC2 instance running 24/7 as a background systemd service.

---

## Step 1: Launch an AWS EC2 Instance

1. Log into your **AWS Management Console** and navigate to **EC2**.
2. Click **Launch Instance**.
3. Configure the following instance details:
   * **Name**: `Trading-Brain-Runner`
   * **Application and OS Images (AMI)**: Select **Ubuntu** (Ubuntu Server 22.04 LTS or 24.04 LTS) OR **Amazon Linux** (Amazon Linux 2023 AMI) — **Free Tier Eligible**.
     > [!WARNING]
     > **Do NOT select any AMI that mentions "Microsoft SQL Server"**. Our bot runs on lightweight Python with SQLite. SQL Server requires expensive `t3.medium` or larger instances and is not needed.
   * **Instance Type**: `t3.micro` or `t4g.micro` (**Free Tier Eligible**)
   * **Key Pair**: Select or create an SSH key pair (e.g. `trading-key.pem`)
   * **Network Settings**:
     * Allow SSH traffic (`Port 22`) from your IP address.
     * No inbound web server ports required (outbound HTTPS is sufficient).
   * **Storage**: Default 8 GB General Purpose SSD (gp3).
4. Click **Launch Instance**.

---

## Step 2: Connect & Upload Code to EC2

Connect to your EC2 instance via SSH:
```bash
ssh -i /path/to/trading-key.pem ubuntu@<YOUR-EC2-PUBLIC-IP>
```

Clone your GitHub repository into `/opt/trading_brain`:
```bash
sudo mkdir -p /opt/trading_brain
sudo chown -R $USER:$USER /opt/trading_brain

# Clone your repository (or copy repository files)
git clone https://github.com/<YOUR_USERNAME>/<YOUR_REPO>.name /opt/trading_brain
cd /opt/trading_brain/.github
```

---

## Step 3: Configure Environment Variables (`.env`)

Create your `.env` credentials file on the EC2 server:
```bash
nano /opt/trading_brain/.env
```
Paste your Trading 212 API credentials:
```env
TRADING212_API_KEY=your_trading212_api_key_here
TRADING212_API_SECRET=your_trading212_api_secret_here
TRADING212_DEMO=true
```
Save and close (`Ctrl+O`, `Enter`, `Ctrl+X`). Secure permissions:
```bash
chmod 600 /opt/trading_brain/.env
```

---

## Step 4: Run Automated Setup Script

Execute the automated provisioning script:
```bash
cd /opt/trading_brain/.github
chmod +x setup_ec2.sh
./setup_ec2.sh
```

---

## Step 5: Test Execution & Start 24/7 Systemd Daemon

### 1. Test Single Connection Scan Mode
Run a dry-run connection test to verify API access and metadata lookups:
```bash
/opt/trading_brain/venv/bin/python /opt/trading_brain/.github/automate_strategy_b_trading212.py --once --dry-run
```

### 2. Start the Systemd Service
Start the service and enable auto-restart on instance reboot:
```bash
sudo systemctl start trading_brain
sudo systemctl status trading_brain
```

### 3. Check Live Logs
Stream real-time bot output using `journalctl`:
```bash
sudo journalctl -u trading_brain -f
```

---

## Service Management Cheat Sheet

| Command | Purpose |
| :--- | :--- |
| `sudo systemctl status trading_brain` | Check status of the trading service |
| `sudo systemctl start trading_brain` | Start the trading service |
| `sudo systemctl stop trading_brain` | Stop the trading service |
| `sudo systemctl restart trading_brain` | Restart after updating code |
| `sudo journalctl -u trading_brain -f` | Tail live real-time output logs |
| `sqlite3 /opt/trading_brain/trading_brain.db "SELECT * FROM trades;"` | Query database active trades |
