# Permissions and limits

- Tested profile only: Strimzi 1.3.0, Kafka 4.3.1, separate broker/controller
  pools, metadata.version 30, eligible.leader.replicas.version 1. Combined roles,
  other operators/cloud distributions and mixed-version upgrades are unsupported.
- Broker identity uses Kafka/Strimzi cluster ID, node-pool status, Pod labels,
  PodSet/pool owner UIDs and actual spec.nodeName. No pod-ordinal inference.
- Two collections detect identified topic-ID, partition, configuration, feature,
  generation and relevant Pod/placement changes. They do not create an atomic
  Kafka/Kubernetes snapshot or prove the absence of intermediate changes.
- Missing evidence is scoped to affected rows where possible. Authentication,
  required resource collection and cluster identity errors are command errors.
- Explicit topic min ISR overrides are read directly. Inherited defaults also
  require describe-config access to the selected replica brokers and agreement
  between those broker values. Missing access returns UNKNOWN; divergent values
  are inconsistent evidence. The tool does not assume a universal default of 1.
- Same-node replicas are a placement fact, not automatically a policy violation.
  No zone/rack policy engine, controller-quorum prediction, election simulation,
  RTO estimate, storage durability assertion or production SLA.
- First/fresh diagnostic connections can time out during a fault even when an
  existing client can still obtain metadata. The lab retains such failures.
- Kafka client properties are supplied by the user. TLS hostname verification
  cannot be disabled through the CLI. Kubeconfig insecure-skip-tls-verify is
  rejected. Credentials/raw configuration are not included in reports.
- Request collection has one shared deadline. Process/JVM startup and cleanup
  add overhead; kubeconfig external authentication plugins have their own
  behavior and are not independently certified by this prototype.
- Live-cluster tests used Linux ARM64 JVMs in Docker/kind on macOS ARM64.
  Unit/request tests and CLI startup also passed on native macOS Java 21 and
  in Linux CI; this does not certify all platform/version combinations.

## Permission profile

Kubernetes: selected namespace get/list on Kafka, KafkaNodePool, Pod and
StrimziPodSet only. `lab/reader.yaml` is an example. No Secret, Node, watch,
exec/logs, mutation or eviction permission is required by the CLI.

Kafka: the tested fixture grants cluster Describe plus selected-topic
Describe/DescribeConfigs; this is not a proof that every granted read permission
is irreducibly necessary for every protocol/API version. For
inherited topic min ISR, describing the relevant broker configurations also
requires the corresponding DescribeConfigs authorization. No Read/Write,
Create/Delete, Alter/AlterConfigs or consumer-group permissions are required.

The lab administrator has wider permissions to provision credentials, copy
artifacts and induce faults. Those permissions are not given to the inspector.

Topic override/default semantics were checked against the
[Kafka topic configuration documentation](https://kafka.apache.org/43/configuration/topic-configs/).
