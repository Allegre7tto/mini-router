#!/bin/bash
set -euo pipefail

KIND_VERSION="0.32"
CLUSTER="${CLUSTER:-mini-router-test}"

echo "=== Creating kind cluster: $CLUSTER ==="
kind create cluster --name "$CLUSTER" --wait 120s

echo "=== Installing Gateway API CRDs ==="
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml

echo "=== Installing Inference Extension CRDs ==="
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/v1.5.0/manifests.yaml

echo "=== Loading images ==="
kind load docker-image mini-router:test --name "$CLUSTER"
kind load docker-image mini-router-adapter:test --name "$CLUSTER"
kind load docker-image mini-router-mock:test --name "$CLUSTER"

echo "=== Applying manifests ==="
kubectl create namespace mini-router --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/k8s/ -n mini-router

echo "=== Waiting for readiness ==="
kubectl wait --for=condition=ready pod -l app=router -n mini-router --timeout=120s
kubectl wait --for=condition=ready pod -l app=worker -n mini-router --timeout=120s

echo "=== Setup complete ==="
