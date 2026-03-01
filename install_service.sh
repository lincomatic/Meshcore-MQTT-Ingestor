#!/bin/bash

# MQTT Mirror Service Installation Script
# This script installs the MQTT bridge as a systemd service called mqtt-mc-ingestor

set -e  # Exit on any error

SERVICE_NAME="mqtt-mc-ingestor"
INSTALL_DIR="/opt/mqtt-mc-ingestor"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_USER="mqtt-mc-ingestor"

echo "========================================"
echo "MQTT Mirror Service Installer"
echo "========================================"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

echo "Step 1: Creating installation directory..."
mkdir -p "$INSTALL_DIR"

echo "Step 2: Copying files..."
cp mqtt-mc-ingestor.py "$INSTALL_DIR/"
cp database.py "$INSTALL_DIR/"
cp config.ini "$INSTALL_DIR/"
cp requirements.txt "$INSTALL_DIR/"

echo "Step 3: Creating Python virtual environment..."
python3 -m venv "$INSTALL_DIR/venv"

echo "Step 4: Installing Python dependencies in venv..."
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip setuptools wheel
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

echo "Step 5: Creating service user..."
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -r -s /bin/false -d "$INSTALL_DIR" "$SERVICE_USER"
    echo "Created user: $SERVICE_USER"
else
    echo "User $SERVICE_USER already exists"
fi

echo "Step 6: Setting permissions..."
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
chmod 755 "$INSTALL_DIR"
chmod 755 "$INSTALL_DIR/venv"
chmod 755 "$INSTALL_DIR/venv/bin"
chmod 644 "$INSTALL_DIR/mqtt-mc-ingestor.py"
chmod 644 "$INSTALL_DIR/database.py"
chmod 600 "$INSTALL_DIR/config.ini"  # Secure config file with credentials

echo "Step 7: Creating systemd service file..."
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=MQTT Mirror Bridge Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/venv/bin/python mqtt-mc-ingestor.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$INSTALL_DIR

[Install]
WantedBy=multi-user.target
EOF

echo "Step 8: Reloading systemd daemon..."
systemctl daemon-reload

echo "Step 9: Enabling service to start on boot..."
systemctl enable "$SERVICE_NAME"

echo ""
echo "========================================"
echo "Installation Complete!"
echo "========================================"
echo ""
echo "Service has been installed and enabled."
echo ""
echo "Useful commands:"
echo "  Start service:   sudo systemctl start $SERVICE_NAME"
echo "  Stop service:    sudo systemctl stop $SERVICE_NAME"
echo "  Restart service: sudo systemctl restart $SERVICE_NAME"
echo "  Check status:    sudo systemctl status $SERVICE_NAME"
echo "  View logs:       sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "Configuration file: $INSTALL_DIR/config.ini"
echo ""
echo "To start the service now, run:"
echo "  sudo systemctl start $SERVICE_NAME"
echo ""
