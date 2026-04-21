#!/usr/bin/env bash
# Deploy peds-asthma-tracker to Hetzner VPS
# Run: ./deploy.sh

set -euo pipefail

VPS_HOST="root@89.167.19.159"
VPS_DIR="/opt/peds-asthma-tracker"

FILES=(
    asthma_tracker.py
    asthma_tracker_web.py
    ai_pipeline.py
    generate_site.py
    CLAUDE.md
    peds-asthma-tracker.service
    vps-setup.sh
)

echo "Deploying to $VPS_HOST:$VPS_DIR ..."

rsync -avz --progress "${FILES[@]}" "$VPS_HOST:$VPS_DIR/"

echo "Restarting peds-asthma-tracker service ..."
ssh "$VPS_HOST" "sudo systemctl restart peds-asthma-tracker"

echo "Checking service status ..."
ssh "$VPS_HOST" "sudo systemctl status peds-asthma-tracker --no-pager -l" || true

echo "Done."
