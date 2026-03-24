#!/bin/bash
# ============================================================
# Combined Deploy: nemweb-accelerator + energy-copilot
#
# Usage:
#   ./deploy.sh                      # deploy both (nemweb energy-copilot target + energy-copilot dev)
#   ./deploy.sh --target prod        # deploy both to prod (nemweb energy-copilot + energy-copilot prod)
#   ./deploy.sh --app-only           # skip nemweb, deploy energy-copilot only
#   ./deploy.sh --nemweb-only        # deploy nemweb only
#   ./deploy.sh --profile my-profile # override Databricks CLI profile
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NEMWEB_DIR="$(cd "$SCRIPT_DIR/../nemweb-accelerator" 2>/dev/null && pwd || true)"

PROFILE="fe-vm-energy-copilot"
TARGET="dev"
DEPLOY_NEMWEB=true
DEPLOY_APP=true

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="$2"; shift 2 ;;
        --target)
            TARGET="$2"; shift 2 ;;
        --app-only)
            DEPLOY_NEMWEB=false; shift ;;
        --nemweb-only)
            DEPLOY_APP=false; shift ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: ./deploy.sh [--target dev|prod] [--profile <profile>] [--app-only] [--nemweb-only]"
            exit 1 ;;
    esac
done

echo "============================================================"
echo " Combined Deploy"
echo "============================================================"
echo " Profile:       $PROFILE"
echo " App target:    $TARGET"
echo " Deploy nemweb: $DEPLOY_NEMWEB"
echo " Deploy app:    $DEPLOY_APP"
echo "============================================================"
echo ""

# -----------------------------------------------------------
# Step 1: Deploy nemweb-accelerator (energy-copilot target)
# -----------------------------------------------------------
if [ "$DEPLOY_NEMWEB" = "true" ]; then
    if [ -z "$NEMWEB_DIR" ] || [ ! -d "$NEMWEB_DIR" ]; then
        echo "WARNING: nemweb-accelerator not found at ../nemweb-accelerator — skipping nemweb deploy."
        echo "         Clone it alongside this repo or use --app-only to suppress this warning."
    else
        echo "Step 1: Deploying nemweb-accelerator (target: energy-copilot)..."
        (cd "$NEMWEB_DIR" && databricks bundle deploy --target energy-copilot --profile "$PROFILE")
        echo "nemweb-accelerator deployed."
        echo ""
    fi
fi

# -----------------------------------------------------------
# Step 2: Deploy energy-copilot bundle
# -----------------------------------------------------------
if [ "$DEPLOY_APP" = "true" ]; then
    echo "Step 2: Deploying energy-copilot (target: $TARGET)..."
    (cd "$SCRIPT_DIR" && databricks bundle deploy --target "$TARGET" --profile "$PROFILE")
    echo "energy-copilot deployed."
    echo ""

    # Print app URL
    APP_NAME="energy-copilot"
    APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('url','(not available)'))" 2>/dev/null || echo "(not available)")

    echo "============================================================"
    echo " Deploy Complete!"
    echo " App URL: $APP_URL"
    echo "============================================================"
fi
