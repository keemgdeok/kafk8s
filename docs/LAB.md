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

## Supported profile

Strimzi 1.3.0, Kafka 4.3.1, separate broker/controller pools, `metadata.version=30`
and `eligible.leader.replicas.version=1`. Combined roles, other operators/cloud
distributions and mixed-version upgrades are unsupported.

Live-cluster tests used Linux ARM64 JVMs in Docker/kind on macOS ARM64.
Unit/request tests and CLI startup also passed on native macOS Java 21 and
in Linux CI; this does not certify all platform/version combinations.

## Required permissions

Kubernetes: selected namespace get/list on Kafka, KafkaNodePool, Pod and
StrimziPodSet. See [reader role](../lab/reader.yaml). No Secret, Node, watch,
exec/logs or write permission is required by the CLI.

Kafka: the tested fixture grants cluster Describe and selected-topic
Describe/DescribeConfigs. Inherited min ISR also requires DescribeConfigs on
the relevant broker configurations. No data Read/Write, Create/Delete,
Alter/AlterConfigs or consumer-group permissions are required. The fixture
is not a proof that every granted permission is necessary for every API version.

Lab administration needs broader permissions to provision credentials and
induce faults; those permissions are not given to the inspector.

## Collection limits

- Broker identity uses cluster IDs, node-pool status, Pod labels, owner UIDs and
  actual `spec.nodeName`, not pod ordinals. Observation changes are checked
  across two collections, not an atomic Kafka/Kubernetes snapshot.
- Explicit topic min ISR overrides are read directly. Inherited defaults must
  agree across the relevant brokers; unavailable or divergent values produce
  `UNKNOWN`, not an assumed default. See [Kafka topic configuration](https://kafka.apache.org/43/configuration/topic-configs/).
- Incomplete evidence returns `UNKNOWN` where possible. Authentication,
  required resource access and cluster identity errors stop the command.
- TLS verification cannot be disabled. Credentials and raw client properties
  are not included in reports.
- Collection shares one deadline. JVM startup, cleanup and external kubeconfig
  authentication plugins can add time beyond that budget.
