#!/usr/bin/env bash
# One-time VPS provisioning for peds-asthma-tracker (Hetzner)
# Run on VPS as root:  bash /opt/peds-asthma-tracker/vps-setup.sh

set -euo pipefail

echo "=== peds-asthma-tracker VPS setup ==="

# --- System update ---
echo "Updating system packages ..."
apt-get update -y
apt-get upgrade -y

# --- Install Caddy (skip if already present) ---
if ! command -v caddy &>/dev/null; then
    echo "Installing Caddy ..."
    apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
    apt-get update -y
    apt-get install -y caddy
else
    echo "Caddy already installed, skipping."
fi

# --- Create non-root user for the service (if not already exists) ---
echo "Creating bctracker user ..."
if ! id bctracker &>/dev/null; then
    useradd -r -m -s /bin/bash bctracker
fi

# --- Create directory structure ---
echo "Creating /opt/peds-asthma-tracker directories ..."
mkdir -p /opt/peds-asthma-tracker/backups
mkdir -p /opt/peds-asthma-tracker/asthma_tracker_data
chown -R bctracker:bctracker /opt/peds-asthma-tracker

# --- Install systemd service ---
echo "Installing systemd service ..."
cp /opt/peds-asthma-tracker/peds-asthma-tracker.service /etc/systemd/system/peds-asthma-tracker.service
systemctl daemon-reload
systemctl enable peds-asthma-tracker
systemctl start peds-asthma-tracker

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Point asthma.hanlabnw.com A record to this server's public IP"
echo "  2. Add to /etc/caddy/Caddyfile:"
echo ""
echo "     asthma.hanlabnw.com {"
echo "         reverse_proxy localhost:8053"
echo "     }"
echo ""
echo "  3. sudo systemctl reload caddy"
echo "  4. Visit https://asthma.hanlabnw.com"
echo ""
echo "Useful commands:"
echo "  journalctl -u peds-asthma-tracker -f        # live logs"
echo "  systemctl status peds-asthma-tracker        # service status"
echo "  systemctl restart peds-asthma-tracker       # restart after deploy"
echo ""
