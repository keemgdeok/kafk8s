#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
test "$(kubectl config current-context)" = kind-kafk8s-lab
test "$(kubectl -n kafk8s-lab get kafka demo -o jsonpath='{.status.kafkaVersion}')" = 4.3.1
umask 077
mkdir -p .tools/tls
kubectl -n kafk8s-lab get secret demo-clients-ca -o json | jq -r '.data["ca.key"]' | openssl base64 -d -A > .tools/tls/ca.key
kubectl -n kafk8s-lab get secret demo-clients-ca-cert -o json | jq -r '.data["ca.crt"]' | openssl base64 -d -A > .tools/tls/clients-ca.crt
kubectl -n kafk8s-lab get secret demo-cluster-ca-cert -o json | jq -r '.data["ca.crt"]' | openssl base64 -d -A > .tools/tls/ca.crt
openssl req -new -newkey rsa:2048 -nodes -keyout .tools/tls/client.key -out .tools/tls/client.csr -subj /CN=kafk8s-reader 2> .tools/tls/generate.log
openssl x509 -req -in .tools/tls/client.csr -CA .tools/tls/clients-ca.crt -CAkey .tools/tls/ca.key -CAserial .tools/tls/clients-ca.srl -CAcreateserial -out .tools/tls/client.crt -days 2 -sha256 2>> .tools/tls/generate.log
openssl pkcs12 -export -inkey .tools/tls/client.key -in .tools/tls/client.crt -out .tools/tls/client.p12 -passout pass:lab-temporary
openssl pkcs12 -in .tools/tls/client.p12 -nodes -out .tools/tls/client.pem -passin pass:lab-temporary
kubectl -n kafk8s-lab create secret generic kafk8s-client-tls --from-file=ca.crt=.tools/tls/ca.crt --from-file=client.pem=.tools/tls/client.pem --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kafk8s-lab create configmap kafk8s-client-properties --from-file=client.properties=lab/client-tls.properties --from-file=bad-ca.properties=lab/client-bad-ca.properties --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kafk8s-lab patch kafka demo --type merge -p '{"spec":{"kafka":{"authorization":{"type":"simple","superUsers":["ANONYMOUS"]}}}}'
echo 'Lab anonymous principal remains superuser for fixture administration; this is not a production auth configuration.'
bash lab/finish-auth.sh
