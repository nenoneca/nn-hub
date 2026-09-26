#!/bin/bash
# nn-gw-agent — camera-as-gateway agent for LINUX cameras.
#
# Every tick: detect whether an NCP radio is attached, report capability
# to the hub, and mirror the dormant gateway identity locally (the same
# thing ESP cameras store in NVS from the CONFIG TLV).  Actually RUNNING
# gw_linux when supported+enabled is the next stage; this agent gives the
# hub truthful capability state and pre-positions the credentials.
#
#   HUB=<hub address> CAM=cam3 nn-gw-agent.sh     (HUB defaults to the provisioned hub_host)
set -u
KV=${NN_KV:-/var/lib/nn/kv/nnprov}
HUB=${HUB:-$(cat "$KV/hub_host" 2>/dev/null || true)}
[ -n "$HUB" ] || { echo "nn-gw-agent: no HUB and no hub_host in $KV -- idle"; exit 0; }
CAM=${CAM:-cam3}
API="http://$HUB:8769/api/v1/cameras/$CAM/gateway"
CRED_DIR=${CRED_DIR:-/var/lib/nn-gw}

# NCP = an Espressif USB-serial that is NOT one of this host's cameras.
# On a camera host the CSI camera is not USB, so any Espressif serial
# device is a candidate radio.
NCP=$(ls /dev/serial/by-id/*Espressif* 2>/dev/null | head -1)
if [ -n "$NCP" ]; then
  BODY='{"supported": true, "reason": "NCP detected: '"$(basename "$NCP")"'"}'
else
  BODY='{"supported": false, "reason": "no NCP detected"}'
fi
curl -sf -m 10 -X POST "$API/report" -H 'Content-Type: application/json' \
     -d "$BODY" >/dev/null || exit 0   # hub unreachable: report next tick

# Mirror the dormant identity locally (idempotent).
mkdir -p "$CRED_DIR" 2>/dev/null || true
if [ ! -s "$CRED_DIR/gateway.json" ]; then
  curl -sf -m 10 "$API/config" -o "$CRED_DIR/gateway.json.tmp" \
    && mv "$CRED_DIR/gateway.json.tmp" "$CRED_DIR/gateway.json" \
    && chmod 600 "$CRED_DIR/gateway.json" \
    && echo "gateway identity stored (dormant) in $CRED_DIR/gateway.json"
fi

# ── auto bring-up: NCP present + role enabled → run the gateway ──────────
[ -n "$NCP" ] || exit 0
EN=$(curl -sf -m 10 "$API" | python3 -c "import sys,json;print(1 if json.load(sys.stdin).get('enabled') else 0)" 2>/dev/null)
[ "$EN" = "1" ] || { systemctl is-active nn-gw >/dev/null 2>&1 && systemctl stop nn-gw; exit 0; }
command -v gw_linux >/dev/null || { echo "gw_linux binary missing — install to /usr/local/bin"; exit 0; }

# One systemd unit, same supervise pattern as the OPi gateway: gw_linux
# self-selects provision-net (incomplete state) vs operate.
if [ ! -f /etc/systemd/system/nn-gw.service ]; then
  cat > /etc/systemd/system/nn-gw.service <<UNIT
[Unit]
Description=nn gateway (camera-hosted, NCP over USB)
After=network-online.target
[Service]
Environment=NCP_UART=auto
# TWO different env vars, learned the hard way (2026-08-28): the BINARY
# honors NN_GW_STATE_DIR (kvstore root), the SUPERVISE SCRIPT honors
# STATE_DIR (its file check).  With neither set and no HOME in the unit,
# kvstore fell back to /tmp (tmpfs — provisioning lost on reboot) while
# supervise checked /root/.local — permanently "not yet provisioned".
Environment=NN_GW_STATE_DIR=/var/lib/nn-gw
Environment=STATE_DIR=/var/lib/nn-gw
ExecStart=/usr/local/bin/gw-supervise
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
fi
systemctl is-active nn-gw >/dev/null 2>&1 || systemctl start nn-gw

# Unprovisioned gateway listens on 8770 — ask the hub to provision it.
GWSTATE=/var/lib/nn-gw/kv/gw_provision
if [ ! -s "$GWSTATE/ot_dataset" ]; then
  sleep 3   # let provision-net open its listener
  MYIP=$(ip -4 route get "$HUB" 2>/dev/null | grep -oE 'src [0-9.]+' | awk '{print $2}')
  [ -n "$MYIP" ] || exit 0
  R=$(curl -sf -m 60 -X POST "http://$HUB:8769/api/v1/gateways/provision-net"         -H 'Content-Type: application/json'         -d "{\"addr\": \"$MYIP:8770\", \"name\": \"$CAM-gw\"}")
  echo "provision-net: ${R:-failed (will retry next tick)}"
  # gw-supervise's Restart=always flips it into operate once provisioned.
  systemctl restart nn-gw
fi
