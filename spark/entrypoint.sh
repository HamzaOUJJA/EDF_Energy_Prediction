#!/bin/sh
# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/entrypoint.sh — Prepare Ivy cache (Docker volume often root:root) and execute as spark user
# =======================================================================

set -e

mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars
chown -R spark:spark /home/spark/.ivy2 2>/dev/null || true

if [ "$(id -u)" = "0" ]; then
  exec gosu spark "$@"
fi

exec "$@"
