#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
case "${1:-}" in
  spread) target=kafk8s-lab-worker2 ;;
  colocated) target=kafk8s-lab-worker ;;
  *) echo 'Usage: bash lab/layout.sh spread|colocated' >&2; exit 2 ;;
esac
patch=$(jq -nc --arg node "$target" '[{op:"replace",path:"/spec/template/pod/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/0/values",value:[$node]}]')
kubectl -n kafk8s-lab patch kafkanodepool b7 --type json -p "$patch"
deadline=$((SECONDS + 240))
while (( SECONDS < deadline )); do
  if kubectl -n kafk8s-lab get pod demo-b7-7 -o json | jq -e --arg node "$target" '.spec.nodeName == $node and any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null; then
    if kubectl -n kafk8s-lab get kafkanodepool b7 -o json | jq -e '.metadata.generation == .status.observedGeneration' >/dev/null; then
      if kubectl -n kafk8s-lab exec lab-client -- java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp '/tmp/probe/classes:/tmp/probe/lib/*' lab.Probe metadata demo-kafka-bootstrap:9092 lab-events | jq -e 'all(.partitions[]; (.isr | sort) == [3,7,11])' >/dev/null; then
        echo "Broker 7 Ready on $target; pool reconciled and full ISR verified."
        exit 0
      fi
    fi
  fi
  sleep 3
done
echo 'Layout change did not become ready within 240 seconds.' >&2
exit 2
