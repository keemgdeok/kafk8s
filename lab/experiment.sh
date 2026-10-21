#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
processes=$(kubectl -n kafk8s-lab exec lab-client -- ps -eo comm,args)
if echo "$processes" | rg -q '^[[:space:]]*java[[:space:]].*lab\.Probe traffic'; then
  echo 'A previous traffic probe is still live; wait for it before another run.' >&2
  exit 2
fi
case "${1:-}" in spread|colocated) layout=$1 ;; *) echo 'spread|colocated required' >&2; exit 2 ;; esac
target=kafk8s-lab-worker
test "$(docker inspect -f '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$target")" = kafk8s-lab
test "$(docker inspect -f '{{.State.Running}}' "$target")" = true
pods=$(kubectl -n kafk8s-lab get pods -o json)
echo "$pods" | jq -e --arg target "$target" 'all(.items[] | select(.spec.nodeName == $target); .metadata.labels["strimzi.io/broker-role"] == "true")' >/dev/null
echo "$pods" | jq -e 'all(.items[] | select(.metadata.labels["strimzi.io/controller-role"] == "true"); any(.status.conditions[]?; .type == "Ready" and .status == "True"))' >/dev/null
test "$(echo "$pods" | jq '[.items[] | select(.metadata.labels["strimzi.io/controller-role"] == "true")] | length')" = 3
bootstrap=demo-b3-3.demo-kafka-brokers.kafk8s-lab.svc:9092,demo-b7-7.demo-kafka-brokers.kafk8s-lab.svc:9092,demo-b11-11.demo-kafka-brokers.kafk8s-lab.svc:9092
probe() { kubectl -n kafk8s-lab exec lab-client -- java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp '/tmp/probe/classes:/tmp/probe/lib/*' lab.Probe "$@"; }
baseline=$(probe metadata "$bootstrap" lab-events)
echo "$baseline" | jq -e '.min_isr == "2" and (.partitions | length) == 3 and all(.partitions[]; (.isr | sort) == [3,7,11])' >/dev/null
on_target=$(echo "$pods" | jq --arg target "$target" '[.items[] | select(.spec.nodeName == $target and .metadata.labels["strimzi.io/broker-role"] == "true")] | length')
if [[ "$layout" == spread ]]; then test "$on_target" = 1; else test "$on_target" = 2; fi
run="$(date -u +%Y%m%dT%H%M%SZ)-$layout"
out="lab/runtime/evidence/$run"
mkdir -p "$out"
echo "$baseline" > "$out/before-metadata.jsonl"
snapshot() {
  kubectl -n kafk8s-lab get kafka demo -o json > "$out/$1-kafka.json"
  kubectl -n kafk8s-lab get kafkanodepool -o json > "$out/$1-pools.json"
  kubectl -n kafk8s-lab get pods -o json > "$out/$1-pods.json"
  kubectl -n kafk8s-lab get strimzipodset -o json > "$out/$1-podsets.json"
}
snapshot before
stopped=false
recover() {
  result=$?
  if [[ "$stopped" == true ]]; then docker start "$target" >/dev/null; fi
  if (( result != 0 )); then
    jq -n --argjson code "$result" '{status:"INCONCLUSIVE",exit_code:$code}' > "$out/run-status.json"
    if [[ -n "${traffic_pid:-}" ]]; then wait "$traffic_pid" || true; fi
  fi
}
trap recover EXIT
trap 'exit 130' INT TERM
probe traffic "$bootstrap" lab-events "$run" 180 > "$out/traffic.jsonl" 2> "$out/traffic-stderr.log" &
traffic_pid=$!
echo "Run $run: 35 seconds baseline, 65 seconds abrupt node loss, then recovery."
sleep 35
kill -0 "$traffic_pid"
jq -e 'select(.event == "READY")' "$out/traffic.jsonl" >/dev/null
stop_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
stopped=true
docker stop --time 0 "$target" >/dev/null
fault_deadline=$((SECONDS + 65))
echo "Stopped only $target at $stop_at"
sleep 40
if ! probe metadata "$bootstrap" lab-events > "$out/during-metadata.jsonl" 2> "$out/during-metadata-stderr.log"; then
  echo 'During-fault metadata probe failed; retain error and continue recovery.' >&2
fi
snapshot during
remaining=$((fault_deadline - SECONDS))
if (( remaining > 0 )); then sleep "$remaining"; fi
start_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker start "$target" >/dev/null
stopped=false
echo "Restarted $target at $start_at"
kubectl wait --for=condition=Ready node/"$target" --timeout=90s
wait "$traffic_pid"
probe metadata "$bootstrap" lab-events > "$out/after-metadata.jsonl"
snapshot after
jq -n --arg layout "$layout" --arg target "$target" --arg stopped "$stop_at" --arg restarted "$start_at" '{layout:$layout,target:$target,stopped_at:$stopped,restarted_at:$restarted,kind_cluster:"kafk8s-lab",fault:"abrupt worker container stop; not pod deletion"}' > "$out/phases.json"
jq -s --arg stop "$stop_at" --arg start "$start_at" '
  def phase: if .at < $stop then "before" elif .at < $start then "during" else "after" end;
  {phases: ([.[] | select(.event == "ACK" or .event == "SEND_ERROR") | . + {phase:phase}] | group_by(.phase) | map({phase:.[0].phase,acked:map(select(.event=="ACK"))|length,errors:map(select(.event=="SEND_ERROR"))|length,error_types:map(select(.event=="SEND_ERROR")|.type)|unique})),summary:map(select(.event=="SUMMARY")),metadata:map(select(.event=="METADATA"))}
' "$out/traffic.jsonl" > "$out/results.json"
jq '.phases,.summary' "$out/results.json"
jq -e '.summary | length == 1 and .[0].consumer_failed == false and (.[0].acked_not_observed_within_window | length) == 0' "$out/results.json" >/dev/null
jq -e '.partitions | all(.[]; (.isr | sort) == [3,7,11])' "$out/after-metadata.jsonl" >/dev/null
jq -n '{status:"COMPLETED",scope:"bounded lab observation, not a no-loss or production guarantee"}' > "$out/run-status.json"
echo "Raw evidence: $out"
