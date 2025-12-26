#!/bin/bash
set -e

SERVICE_NAME="file-uploader"

echo "🔧 Installing $SERVICE_NAME service and timer..."

# Copy service + timer files into systemd
sudo cp ./${SERVICE_NAME}.service /etc/systemd/system/
sudo cp ./${SERVICE_NAME}.timer /etc/systemd/system/

# Reload systemd so it sees them
sudo systemctl daemon-reload

# Enable + start timer
sudo systemctl enable --now ${SERVICE_NAME}.timer

echo "$SERVICE_NAME service installed and timer started."
echo "Logs available via: journalctl -u ${SERVICE_NAME}.service -f"