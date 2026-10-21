# Disposable lab

Prerequisites: Docker with at least the tested 8 GiB shared memory/10 CPUs,
kubectl, bash, curl, jq, rg, openssl and shasum. Leave room for other services.
Images/downloads use several GiB and cold startup can take several minutes.
Do not run concurrent builds against the same Gradle cache.

All commands below are from this repository. kind writes only
`lab/runtime/kubeconfig`; it does not change the default kubeconfig. Existing
services are not stopped. Pinned versions/digests are in `lab/versions.properties`.

```sh
bash lab/bootstrap.sh
bash lab/setup.sh
bash lab/prepare-probe.sh
bash lab/layout.sh spread
bash lab/run.sh spread
bash lab/layout.sh colocated
bash lab/run.sh colocated
```

`lab/run.sh` validates and snapshots the experiment script before execution.
Each run captures before/during/after metadata and Kubernetes objects under
`lab/runtime/evidence/` (gitignored). It runs a low-rate synthetic producer/consumer
for 180 seconds, stops only `kafk8s-lab-worker` after approximately 35 seconds, and restarts it
after approximately 65 seconds of node loss. The EXIT trap restores that worker
on failures. A run is not a throughput benchmark.

Selected reference runs are preserved in [lab/evidence](../lab/evidence/).

Targets are checked against kind cluster labels, actual broker placement and
controller readiness. A live previous traffic probe prevents another run.
Never substitute a production context or node. Three controllers share worker4
to isolate broker failure; this is not a production HA layout. Single-host kind
does not reproduce independent physical hosts or availability zones.

## Run the CLI in the lab

After `bash lab/build.sh`, create the restricted Kubernetes reader and copy the
distribution. The lab-only anonymous Kafka listener is for initial experiments.

```sh
export KUBECONFIG="$PWD/lab/runtime/kubeconfig"
kubectl apply -f lab/reader.yaml
kubectl -n kafk8s-lab wait --for=condition=Ready pod/lab-reader --timeout=120s
kubectl -n kafk8s-lab cp build/install/kafk8s lab-reader:/tmp/kafk8s
kubectl -n kafk8s-lab exec lab-reader -- /tmp/kafk8s/bin/kafk8s check \
  --namespace kafk8s-lab --cluster demo \
  --bootstrap-server demo-kafka-bootstrap:9092 \
  --topic lab-events --lost-node kafk8s-lab-worker --format json
```

A risk result intentionally returns exit 1. Check JSON, not just shell success.

## Restricted Kafka principal

`bash lab/enable-auth.sh` generates a two-day lab client certificate from this
lab's clients CA, mounts credentials in a separate reader Pod, and enables
Kafka's simple authorizer. CA/private keys remain gitignored in `.tools/tls`;
the inspector never requests Secrets. This script is a lab administrator action,
not functionality of the inspector. The lab anonymous principal remains a
superuser for setup; **never use this auth layout in production**.

Enabling the authorizer rolls controllers/brokers and can exceed cold-start
timeouts. A wait timeout is not proof of terminal failure: inspect the same
Kafka resource and its observedGeneration before continuing. Do not begin an
experiment during a pending rollout. After a timeout, `bash lab/finish-auth.sh`
continues against that same rollout without generating new certificates.

After the TLS reader is provisioned, `bash lab/verify-cli.sh` checks the actual
CLI with scoped credentials, both layouts, incomplete evidence, errors and TLS
failures. It creates only lab-prefixed fixture topics, temporarily denies access
to its dedicated principal and restores that Deny on exit. It leaves the brokers
in the spread layout. Results and exit codes are saved under `lab/runtime/evidence/`.

No automated broad cleanup is provided. Preserve any needed evidence and stop
only this lab's explicitly identified containers when finished; never use
`docker system prune` or delete other Kubernetes contexts.
