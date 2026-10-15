#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
docker run --rm --memory 1g -v "$PWD:/workspace" -w /workspace -e GRADLE_USER_HOME=/workspace/.tools/gradle-home eclipse-temurin:21-jdk@sha256:f9bf7e9fe2f9bc4657537c1a7e20c768f0c3bafca41598263be4714783dd62eb ./gradlew --no-daemon labBundle
kubectl -n kafk8s-lab wait --for=condition=Ready kafka/demo --timeout=300s
kubectl -n kafk8s-lab wait --for=condition=Ready pod/lab-client --timeout=120s
kubectl -n kafk8s-lab cp build/lab lab-client:/tmp/probe
kubectl -n kafk8s-lab exec demo-b3-3 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server demo-kafka-bootstrap:9092 --create --if-not-exists --topic lab-events --partitions 3 --replication-factor 3 --config min.insync.replicas=2
