#!/bin/bash

# MQTT MC Ingestor Service Uninstallation Script
# This script removes the mqtt-mc-ingestor systemd service

set -e  # Exit on any error

SERVICE_NAME="mqtt-mc-ingestor"
INSTALL_DIR="/opt/mqtt-mc-ingestor"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_USER="mqtt-mc-ingestor"

echo "========================================"
echo "MQTT MC Ingestor Service Uninstaller"
echo "========================================"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

# Confirm uninstallation
read -p "This will remove the mqtt-mc-ingestor service and all files. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstallation cancelled."
    exit 0
fi

echo "Step 1: Stopping service (if running)..."
if systemctl is-active --quiet "$SERVICE_NAME"; then
    systemctl stop "$SERVICE_NAME"
    echo "Service stopped."
else
    echo "Service is not running."
fi

echo "Step 2: Disabling service..."
if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
    systemctl disable "$SERVICE_NAME"
    echo "Service disabled."
else
    echo "Service is not enabled."
fi

echo "Step 3: Removing service file..."
if [ -f "$SERVICE_FILE" ]; then
    rm -f "$SERVICE_FILE"
    echo "Service file removed: $SERVICE_FILE"
else
    echo "Service file not found: $SERVICE_FILE"
fi

echo "Step 4: Reloading systemd daemon..."
systemctl daemon-reload
systemctl reset-failed

echo "Step 5: Removing installation directory..."
if [ -d "$INSTALL_DIR" ]; then
    rm -rf "$INSTALL_DIR"
    echo "Installation directory removed: $INSTALL_DIR"
else
    echo "Installation directory not found: $INSTALL_DIR"
fi

echo "Step 6: Removing service user..."
if id "$SERVICE_USER" &>/dev/null; then
    userdel "$SERVICE_USER"
    echo "User removed: $SERVICE_USER"
else
    echo "User not found: $SERVICE_USER"
fi

echo ""
echo "========================================"
echo "Uninstallation Complete!"
echo "========================================"
echo ""
echo "The mqtt-mc-ingestor service has been completely removed."
echo ""
