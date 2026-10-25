#!/bin/bash
set -euo pipefail

# kind has no external load balancer; use the Envoy Service inside the cluster.
echo "=== Finding gateway Service ==="
addr=$(kubectl get service -n envoy-gateway-system \
    -l gateway.envoyproxy.io/owning-gateway-namespace=mini-router,gateway.envoyproxy.io/owning-gateway-name=envoy-gateway \
    -o jsonpath='{.items[0].spec.clusterIP}')

if [ -z "$addr" ]; then
    echo "gateway Service has no ClusterIP" >&2
    exit 1
fi
echo "Gateway: $addr"

echo "=== Testing health ==="
kubectl get pod -n mini-router -l app=router -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.name}{"="}{.ready}{" "}{end}{"\n"}{end}'

echo "=== Testing chat completion ==="
body=$(kubectl run client -n mini-router --rm -i --restart=Never \
    --image=curlimages/curl:8.12.1 --command -- \
    curl -fsS -H 'content-type: application/json' \
    --data '{"model":"fixed","messages":[{"role":"user","content":"hello"}]}' \
    "http://$addr/v1/chat/completions")
echo "$body"
echo "$body" | grep -q 'mock response'

echo "=== Testing metrics ==="
metrics=$(kubectl run metrics -n mini-router --rm -i --restart=Never \
    --image=curlimages/curl:8.12.1 --command -- \
    curl -fsS http://router-metrics:9001/metrics)
echo "$metrics"
echo "$metrics" | grep -Eq 'router_requests_total [1-9][0-9]*$'
echo "$metrics" | grep -q 'router_active_endpoints 2'

echo "=== Verification complete ==="
