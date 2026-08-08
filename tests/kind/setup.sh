#!/bin/bash
set -euo pipefail

CLUSTER="${CLUSTER:-mini-router-test}"

if ! kind get clusters | grep -qx "$CLUSTER"; then
    echo "=== Creating kind cluster: $CLUSTER ==="
    kind create cluster --name "$CLUSTER" --wait 120s
fi

echo "=== Installing Gateway API CRDs ==="
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.0/standard-install.yaml

echo "=== Installing Inference Extension CRDs ==="
kubectl apply --server-side -k "https://github.com/kubernetes-sigs/gateway-api-inference-extension/config/crd?ref=v1.5.0"

echo "=== Installing Envoy Gateway ==="
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm \
    --version v1.8.0 \
    --namespace envoy-gateway-system \
    --create-namespace
kubectl wait --for=condition=available deployment/envoy-gateway \
    -n envoy-gateway-system --timeout=180s

echo "=== Loading images ==="
kind load docker-image mini-router:test --name "$CLUSTER"
kind load docker-image mini-router-adapter:test --name "$CLUSTER"
kind load docker-image mini-router-mock:test --name "$CLUSTER"

echo "=== Applying manifests ==="
kubectl create namespace mini-router --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic router -n mini-router --from-literal=token=test-token \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f tests/kind/gateway.yaml -n mini-router
kubectl apply -f deploy/k8s/ -n mini-router

echo "=== Waiting for readiness ==="
kubectl wait --for=condition=ready pod -l app=router -n mini-router --timeout=120s
kubectl wait --for=condition=ready pod -l app=worker -n mini-router --timeout=120s
kubectl wait --for=condition=Programmed gateway/envoy-gateway -n mini-router --timeout=180s

echo "=== Setup complete ==="
