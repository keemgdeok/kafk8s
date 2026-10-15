#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
deadline=$((SECONDS + 600))
while ! kubectl --request-timeout=10s -n kafk8s-lab get kafka demo -o json | jq -e '.status.observedGeneration == .metadata.generation and any(.status.conditions[]?; .type=="Ready" and .status=="True")' >/dev/null; do
  if (( SECONDS >= deadline )); then echo 'Same Kafka rollout did not reconcile within 600s; inspect it, do not recreate it.' >&2; exit 2; fi
  sleep 5
done
acl() { kubectl -n kafk8s-lab exec demo-b11-11 -- /opt/kafka/bin/kafka-acls.sh --bootstrap-server demo-b11-11.demo-kafka-brokers:9092 "$@"; }
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --cluster
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --operation DescribeConfigs --topic lab-events
kubectl apply -f lab/reader-tls.yaml
kubectl -n kafk8s-lab wait --for=condition=Ready pod/lab-reader-tls --timeout=120s
kubectl -n kafk8s-lab cp build/install/kafk8s lab-reader-tls:/tmp/kafk8s
