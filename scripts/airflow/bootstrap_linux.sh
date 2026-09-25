#!/usr/bin/env bash
# Run only inside the approved, dedicated Ubuntu 24.04 WSL distribution.
set -euo pipefail
[[ $(id -u) == 0 ]] || { echo 'Run bootstrap as root inside the dedicated distro'; exit 1; }
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]] || { echo 'Require Ubuntu 24.04'; exit 1; }
project=/mnt/f/project/medical-etl-fhir-platform
base=/opt/medical-etl-airflow
[[ -f "$project/output/scale-stream-final/runs/p10000-stream-local-r1/manifest.json" ]]
mkdir -p "$base/project" "$base/runtime" "$base/runs"
if ! mountpoint -q "$base/project"; then
    mount --bind "$project" "$base/project"
    mount -o remount,bind,ro "$base/project"
fi
findmnt -n -o OPTIONS --target "$base/project" | tr ',' '\n' | grep -qx ro
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y python3.12-venv openjdk-21-jre-headless ca-certificates curl
python3.12 -m venv "$base/runtime/airflow-venv"
python3.12 -m venv "$base/runtime/worker-venv"
curl --fail --location --retry 3 'https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt' -o "$base/runtime/airflow-constraints.txt"
"$base/runtime/airflow-venv/bin/python" -m pip install 'apache-airflow==2.11.2' --constraint "$base/runtime/airflow-constraints.txt"
"$base/runtime/worker-venv/bin/python" -m pip install -r "$base/project/scripts/airflow/requirements-worker.txt"
"$base/runtime/airflow-venv/bin/python" -m pip check
"$base/runtime/worker-venv/bin/python" -m pip check
"$base/runtime/airflow-venv/bin/python" -m pip freeze > "$base/runtime/airflow-freeze.txt"
"$base/runtime/worker-venv/bin/python" -m pip freeze > "$base/runtime/worker-freeze.txt"
printf '%s\n' 'Bootstrap complete. Run the acceptance command documented in docs/stage-h/README.md.'
