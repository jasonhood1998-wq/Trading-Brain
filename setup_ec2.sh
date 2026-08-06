#!/usr/bin/env bash
# ===============================================================================
# STRATEGY B TRADING ENGINE - AUTOMATED AWS EC2 SETUP SCRIPT
# Target OS: Ubuntu 22.04 LTS / Amazon Linux 2023
# ===============================================================================

set -e

echo "======================================================================="
echo "[EC2 SETUP] STARTING TRADING BRAIN DAEMON ENVIRONMENT PROVISIONING"
echo "======================================================================="

# 1. Update system packages and install prerequisites
echo "[1/5] Updating system packages..."
if command -v apt-get &> /dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-pip python3-venv git curl
elif command -v dnf &> /dev/null; then
    sudo dnf update -y
    sudo dnf install -y python3 python3-pip git
fi

# 2. Set up application directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_DIR="${SCRIPT_DIR}"

echo "[2/5] Using application directory at ${APP_DIR}..."

# 3. Create Python Virtual Environment & Install Dependencies
echo "[3/5] Setting up Python virtual environment..."
python3 -m venv ${APP_DIR}/venv
source ${APP_DIR}/venv/bin/activate

echo "[3/5] Installing Python dependencies..."
pip install --upgrade pip
if [ -f "${APP_DIR}/requirements.txt" ]; then
    pip install -r ${APP_DIR}/requirements.txt
else
    pip install requests python-dotenv yfinance pandas_market_calendars tzdata pandas schedule
fi

# 4. Verify .env file
if [ ! -f "${APP_DIR}/.env" ]; then
    echo "[WARNING] ${APP_DIR}/.env file not found."
    echo "[ACTION REQUIRED] Please create ${APP_DIR}/.env with your Trading 212 API credentials:"
    echo "TRADING212_API_KEY=your_key_here"
    echo "TRADING212_API_SECRET=your_secret_here"
    echo "TRADING212_DEMO=true"
fi

# 5. Install Systemd Service
echo "[5/5] Registering systemd daemon service..."
SERVICE_SRC="${APP_DIR}/trading_brain.service"
if [ ! -f "$SERVICE_SRC" ]; then
    SERVICE_SRC="${APP_DIR}/.github/trading_brain.service"
fi
SERVICE_DEST="/etc/systemd/system/trading_brain.service"

if [ -f "$SERVICE_SRC" ]; then
    sudo cp $SERVICE_SRC $SERVICE_DEST
    sudo sed -i "s|User=ubuntu|User=$USER|g" $SERVICE_DEST
    sudo sed -i "s|WorkingDirectory=/opt/trading_brain|WorkingDirectory=${APP_DIR}|g" $SERVICE_DEST
    sudo sed -i "s|/opt/trading_brain/venv/bin/python|${APP_DIR}/venv/bin/python|g" $SERVICE_DEST
    sudo sed -i "s|/opt/trading_brain/automate_strategy_b_trading212.py|${APP_DIR}/automate_strategy_b_trading212.py|g" $SERVICE_DEST
    sudo systemctl daemon-reload
    sudo systemctl enable trading_brain.service
    echo " SUCCESS: Registered systemd service 'trading_brain.service' for user '$USER' at ${APP_DIR}"
    echo " Run 'sudo systemctl start trading_brain' to start continuous execution."
else
    echo "[WARNING] Service file $SERVICE_SRC not found. Skip systemd registration."
fi

echo "======================================================================="
echo "[EC2 SETUP] PROVISIONING COMPLETE!"
echo "======================================================================="
