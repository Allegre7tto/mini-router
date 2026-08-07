#!/bin/bash
set -euo pipefail
CLUSTER="${CLUSTER:-mini-router-test}"
kind delete cluster --name "$CLUSTER"
echo "=== Teardown complete ==="
