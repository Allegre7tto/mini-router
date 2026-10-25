#!/bin/bash
set -euo pipefail
cluster="${CLUSTER:-mini-router-test}"
kind delete cluster --name "$cluster"
echo "=== Teardown complete ==="
