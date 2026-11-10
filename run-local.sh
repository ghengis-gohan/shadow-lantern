#!/usr/bin/env bash
# Run the whole chain on one laptop with no hardware: spectral node in sim mode, ground station,
# and a second (infrasound) node firing every ~25 s. Dashboard at http://localhost:8080
# Needs: python3, numpy (pip install numpy plotly). Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "$0")"
export TELEMETRY_DIR=${TELEMETRY_DIR:-/tmp/lantern}
mkdir -p "$TELEMETRY_DIR"

GS_DB="$TELEMETRY_DIR/ground.db" HTTP_PORT=8080 GS_UDP_PORT=5005 python3 ground/app/gs.py &
sleep 1
NODE_ID=spec-1 SPECTRO_SOURCE=sim UPLINK=udp://127.0.0.1:5005 HTTP_PORT=8081 \
  TELEMETRY_DB="$TELEMETRY_DIR/spectral.db" DARK_FILE="$TELEMETRY_DIR/dark.npy" python3 node/spectral/app/spectral.py &
python3 nodes/infra_node.py --ground 127.0.0.1:5005 --node infra-1 --auto 25 --db "$TELEMETRY_DIR/infra.db" < /dev/null &

echo
echo "  dashboard  http://localhost:8080      node api  http://localhost:8081/latest"
echo "  cut the link:   sudo iptables -I INPUT -p udp --dport 5005 -j DROP   (and -D to restore)"
echo
trap 'kill 0' INT TERM
wait
