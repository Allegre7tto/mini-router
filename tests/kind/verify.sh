#!/bin/bash
set -euo pipefail

GW_ADDR=$(kubectl get gateway envoy-gateway -n mini-router -o jsonpath='{.status.addresses[0].value}')

if [ -z "$GW_ADDR" ]; then
    echo "gateway has no address" >&2
    exit 1
fi

echo "=== Testing health ==="
kubectl get pod -n mini-router -l app=router -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.name}{"="}{.ready}{" "}{end}{"\n"}{end}'

echo "=== Testing chat completion ==="
BODY=$(kubectl run client -n mini-router --rm -i --restart=Never \
    --image=curlimages/curl:8.12.1 --command -- \
    curl -fsS -H 'content-type: application/json' \
    --data '{"model":"fixed","messages":[{"role":"user","content":"hello"}]}' \
    "http://$GW_ADDR/v1/chat/completions")
echo "$BODY" | grep -q 'mock response'

echo "=== Testing metrics ==="
METRICS=$(kubectl run metrics -n mini-router --rm -i --restart=Never \
    --image=curlimages/curl:8.12.1 --command -- \
    curl -fsS http://router-metrics:9001/metrics)
echo "$METRICS" | grep -Eq 'router_requests_total [1-9][0-9]*$'
echo "$METRICS" | grep -q 'router_active_endpoints 2'

echo "=== Verification complete ==="
