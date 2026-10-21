#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
out="lab/runtime/evidence/$(date -u +%Y%m%dT%H%M%SZ)-cli"
mkdir -p "$out"
printf '%s\n' "$out" > lab/runtime/latest-cli-evidence
admin() { kubectl -n kafk8s-lab exec demo-b11-11 -- "$@"; }
acl() { admin /opt/kafka/bin/kafka-acls.sh --bootstrap-server demo-b11-11.demo-kafka-brokers:9092 "$@"; }
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --cluster
acl --remove --force --allow-principal User:CN=kafk8s-reader --operation DescribeConfigs --cluster
acl --remove --force --deny-principal User:CN=kafk8s-reader --operation Describe --topic lab-events
cleanup() { acl --remove --force --deny-principal User:CN=kafk8s-reader --operation Describe --topic lab-events >/dev/null 2>&1 || true; }
trap cleanup EXIT
bash lab/layout.sh colocated
kubectl -n kafk8s-lab cp build/install/kafk8s/lib/kafk8s-0.1.0-SNAPSHOT.jar lab-reader-tls:/tmp/kafk8s/lib/kafk8s-0.1.0-SNAPSHOT.jar
common=(--namespace kafk8s-lab --cluster demo --bootstrap-server demo-kafka-bootstrap:9093 --client-properties /config/client.properties --format json)
expect() {
  local name=$1 expected=$2 predicate=$3
  shift 3
  set +e
  kubectl -n kafk8s-lab exec lab-reader-tls -- /tmp/kafk8s/bin/kafk8s "$@" > "$out/$name.json" 2> "$out/$name.stderr.log"
  local actual=$?
  set -e
  test "$actual" = "$expected" || { echo "$name: expected exit $expected, got $actual" >&2; return 2; }
  jq -e "$predicate" "$out/$name.json" >/dev/null
  jq -n --arg name "$name" --argjson exit "$actual" '{case:$name,exit_code:$exit,verified:true}' >> "$out/checks.jsonl"
  echo "$name verified (exit $actual)"
}
expect inspect 0 '.complete and all(.partitions[]; .status=="OBSERVED")' inspect "${common[@]}" --topic lab-events
ip=$(kubectl -n kafk8s-lab get pod demo-b11-11 -o jsonpath='{.status.podIP}')
expect hostname-denied 2 '.status=="ERROR" and .reason_code=="AUTHORIZATION_ERROR"' inspect --namespace kafk8s-lab --cluster demo --bootstrap-server "$ip:9093" --client-properties /config/client.properties --topic lab-events --format json --timeout 10s
expect bad-ca-denied 2 '.status=="ERROR" and .reason_code=="AUTHORIZATION_ERROR"' inspect --namespace kafk8s-lab --cluster demo --bootstrap-server demo-kafka-bootstrap:9093 --client-properties /config/bad-ca.properties --topic lab-events --format json --timeout 10s
expect colocated 1 '.complete and (.partitions|length)==3 and all(.partitions[]; .status=="RISK" and .remaining_isr==1)' check "${common[@]}" --topic lab-events --lost-node kafk8s-lab-worker
expect invalid-node 2 '.status=="ERROR" and .reason_code=="NODE_NOT_FOUND"' check "${common[@]}" --topic lab-events --lost-node nonexistent-node
expect invalid-argument 2 '.status=="ERROR" and .reason_code=="INVALID_ARGUMENT"' check --format json
for topic in lab-inherited lab-config-denied; do
  admin /opt/kafka/bin/kafka-topics.sh --bootstrap-server demo-b11-11.demo-kafka-brokers:9092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 3
done
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --operation DescribeConfigs --topic lab-inherited
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --topic lab-config-denied
expect config-denied 3 '(.complete|not) and all(.partitions[]; .status=="UNKNOWN" and .reason_code=="CONFIG_UNAVAILABLE")' check "${common[@]}" --topic lab-config-denied --lost-node kafk8s-lab-worker
expect mixed 1 '(.complete|not) and any(.partitions[]; .status=="RISK") and any(.partitions[]; .status=="UNKNOWN")' check "${common[@]}" --topic lab-events --topic lab-config-denied --lost-node kafk8s-lab-worker
expect inherited-no-broker-permission 3 '(.complete|not) and all(.partitions[]; .reason_code=="CONFIG_UNAVAILABLE")' check "${common[@]}" --topic lab-inherited --lost-node kafk8s-lab-worker
acl --add --allow-principal User:CN=kafk8s-reader --operation DescribeConfigs --cluster
expect inherited 1 '.complete and all(.partitions[]; .min_isr==2 and (.config_source|startswith("VERIFIED_BROKER_DEFAULTS")))' check "${common[@]}" --topic lab-inherited --lost-node kafk8s-lab-worker
acl --add --allow-principal User:CN=kafk8s-reader --operation Describe --operation DescribeConfigs --topic lab-missing
expect missing-topic 2 '.status=="ERROR" and .reason_code=="TOPIC_NOT_FOUND"' check "${common[@]}" --topic lab-missing --lost-node kafk8s-lab-worker
acl --add --deny-principal User:CN=kafk8s-reader --operation Describe --topic lab-events
expect authorization-denied 2 '.status=="ERROR" and .reason_code=="AUTHORIZATION_ERROR"' inspect "${common[@]}" --topic lab-events
acl --remove --force --deny-principal User:CN=kafk8s-reader --operation Describe --topic lab-events
set +e
kubectl auth can-i get secrets --as=system:serviceaccount:kafk8s-lab:kafk8s-reader -n kafk8s-lab > "$out/kubernetes-secret-permission.txt"
denied=$?
set -e
test "$denied" = 1
test "$(kubectl auth can-i get pods --as=system:serviceaccount:kafk8s-lab:kafk8s-reader -n kafk8s-lab)" = yes
bash lab/layout.sh spread
expect spread 0 '.complete and all(.partitions[]; .status=="NO_SHORTFALL" and .remaining_isr==2)' check "${common[@]}" --topic lab-events --lost-node kafk8s-lab-worker
expect deadline 2 '.status=="ERROR" and .reason_code=="DEADLINE_EXCEEDED"' inspect --namespace kafk8s-lab --cluster demo --bootstrap-server 127.0.0.1:1 --topic lab-events --timeout 2s --format json
sha_file=build/install/kafk8s/lib/kafk8s-0.1.0-SNAPSHOT.jar
shasum -a 256 "$sha_file" > "$out/artifact.sha256"
jq -s '{status:"VERIFIED",checks:.}' "$out/checks.jsonl" > "$out/verification.json"
echo "CLI evidence: $out"
