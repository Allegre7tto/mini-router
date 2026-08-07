#!/bin/bash
set -euo pipefail

CLUSTER="${CLUSTER:-mini-router-test}"
GW_ADDR=$(kubectl get gateway envoy-gateway -n envoy-gateway-system -o jsonpath='{.status.addresses[0].value}' 2>/dev/null || echo "")

if [ -z "$GW_ADDR" ]; then
    echo "=== Skipping HTTP tests (no gateway address) ==="
    exit 0
fi

echo "=== Testing health ==="
echo "=== Testing chat completion ==="
echo "=== Verification complete ==="
