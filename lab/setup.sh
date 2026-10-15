#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p lab/runtime
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
if .tools/kind get clusters | rg -qx kafk8s-lab; then
  test -f "$KUBECONFIG" || { echo 'Existing lab has no local kubeconfig; refusing.' >&2; exit 2; }
else
  .tools/kind create cluster --config lab/kind.yaml --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0 --kubeconfig "$KUBECONFIG" --wait 120s
fi
test "$(kubectl config current-context)" = kind-kafk8s-lab
kubectl create namespace kafk8s-lab --dry-run=client -o yaml | kubectl apply -f -
for manifest in .tools/strimzi-1.3.0/install/cluster-operator/*.yaml; do
  sed 's/namespace: .*/namespace: kafk8s-lab/' "$manifest" | kubectl apply -n kafk8s-lab -f -
done
kubectl -n kafk8s-lab set resources deployment/strimzi-cluster-operator --requests=cpu=100m,memory=256Mi --limits=cpu=1,memory=512Mi
kubectl -n kafk8s-lab set env deployment/strimzi-cluster-operator JAVA_OPTS='-Xms128m -Xmx256m -XX:ActiveProcessorCount=2'
kubectl -n kafk8s-lab patch deployment strimzi-cluster-operator --type merge -p '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"kafk8s-lab-worker4"}}}}}'
kubectl -n kafk8s-lab rollout status deployment/strimzi-cluster-operator --timeout=300s
kubectl apply -f lab/kafka.yaml
kubectl apply -f lab/client.yaml
