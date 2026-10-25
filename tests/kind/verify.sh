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

for stream in false true; do
    echo "=== Testing chat completion: stream=$stream ==="
    if body=$(kubectl run "client-$stream" -n mini-router --rm -i --restart=Never \
        --image=curlimages/curl:8.12.1 --command -- \
        curl --fail-with-body --silent --show-error --include \
        --connect-timeout 10 --max-time 30 -H 'content-type: application/json' \
        -H 'x-gateway-destination-endpoint: 127.0.0.1:1' -H 'x-mini-router-token: forged' \
        --data "{\"model\":\"fixed\",\"stream\":$stream,\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}" \
        "http://$addr/v1/chat/completions"); then
        echo "$body"
    else
        status=$?
        echo "$body"
        exit "$status"
    fi
    echo "$body" | grep -q 'mock response'
    if [ "$stream" = true ]; then
        echo "$body" | grep -qi 'content-type: text/event-stream'
        echo "$body" | grep -Fq 'data: [DONE]'
    fi

    echo "=== Checking selected Worker token IDs ==="
    worker=$(echo "$body" | awk 'tolower($1) == "x-mock-worker:" {gsub("\r", "", $2); print $2}')
    ip=$(kubectl get pod "$worker" -n mini-router -o jsonpath='{.status.podIP}')
    last=$(kubectl run "inspect-$stream" -n mini-router --rm -i --restart=Never \
        --image=curlimages/curl:8.12.1 --command -- \
        curl -fsS --connect-timeout 10 --max-time 30 "http://$ip:8000/control/last")
    echo "$last"
    echo "$last" | grep -Fq '"prompt_token_ids":[1,2,3,4]'
    if echo "$last" | grep -q '"mini_router"'; then
        echo "adapter did not remove the internal envelope" >&2
        exit 1
    fi
done

echo "=== Testing metrics ==="
# Read both replicas rather than relying on a Service to pick the one used by EPP.
metrics=""
for ip in $(kubectl get pod -n mini-router -l app=router -o jsonpath='{.items[*].status.podIP}'); do
    sample=$(kubectl run metrics -n mini-router --rm -i --restart=Never \
        --image=curlimages/curl:8.12.1 --command -- \
        curl -fsS --connect-timeout 10 --max-time 30 "http://$ip:9001/metrics")
    metrics="$metrics
$sample"
done
echo "$metrics" | grep -q 'router_active_endpoints 2'
echo "$metrics" | awk '
    /^router_requests_total / {requests += $2}
    /^router_endpoint_decisions_total / {decisions += $2}
    /^router_reservation_count / {reservations += $2; replicas++}
    END {
        printf "Requests: %d, decisions: %d, reservations: %g\n", requests, decisions, reservations
        exit !(requests >= 2 && decisions >= 2 && replicas == 2 && reservations == 0)
    }
'

echo "=== Verification complete ==="
