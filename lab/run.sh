#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash -n lab/experiment.sh
bash -c "$(<lab/experiment.sh)" lab/experiment.sh "$@"
