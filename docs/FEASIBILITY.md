# Feasibility record

Started 2026-10-14 on macOS arm64. Docker reports 10 CPUs and 8 GiB shared
memory. Existing Airflow/PostgreSQL containers remain running and are not
experiment targets. kind and Java were not available on the host; kind is
project-local and Java 21 runs in a pinned container.

## Verified identity observations

- Separate controller IDs 0, 1, 2 and broker IDs 3, 7, 11 were assigned.
- KafkaNodePool status includes clusterId, nodeIds, roles and observedGeneration.
- Pod owner is StrimziPodSet; its owner is the KafkaNodePool, not directly Kafka.
- Pool/cluster/role/pod-name labels are available. There is no assumed node-ID label.
- Actual Pod spec.nodeName supplies placement. Pod Ready does not substitute for ISR.

Kafka became Ready with all three brokers. The official Admin client returned
the same cluster ID as Kafka/NodePool status, min ISR 2 from DYNAMIC_TOPIC_CONFIG,
ISR [3,7,11] for all three partitions, eligible.leader.replicas.version 1 and
metadata.version 30. Both completed fault runs and the implementation decision
are recorded in [CASE_STUDY.md](CASE_STUDY.md).

## Setup issues retained

The initial Operator rollout wait expired while images were still downloading.
The resource remained live and subsequently became Ready; it was not recreated
solely because the observation command timed out.

kind's all-platform Docker image preload failed with a missing content digest on
this Docker/arm64 environment. Importing only linux/arm64 via containerd succeeded;
normal Kubernetes image pulls also continued. No version was changed to hide the
issue. Cold image pulls can take several minutes. Setup timeout was increased
to 300 seconds, not made unbounded.

Two early spread runs are explicitly excluded: a standalone Admin timeout caused
the first orchestration abort; editing a live Bash script caused the second.
Both target nodes were restored by the EXIT trap. The next runner takes a complete
script snapshot, validates syntax, rejects an already live traffic probe, and
retains failures instead of treating partial evidence as a completed experiment.

The three controllers share a separate lab worker to isolate broker-node loss.
This is not a production controller HA recommendation. All kind nodes share one
physical host; the experiment is not evidence of multi-AZ failure tolerance.

## Subsequent implementation validation (2026-10-15)

The Java CLI was implemented only after reviewing the two completed layouts and
the manual workflow. Unit/request tests passed (43 final tests, including local
kubeconfig context selection and relative certificate paths). The suite reads the
original run fixtures and exercises conservative failure handling as well as
the actual Kubernetes SDK's request paths against a local HTTP server.

Adding Kafka authorization triggered a rolling change, and the first 300-second
wait expired. The same Kafka resource stayed live. Liveness/readiness timeouts
and CPU contention were observed during parallel builds/rollout. JVM processor
counts were limited to 2 in the lab and builds were serialized/capped; the same
rollout then completed with observedGeneration matching generation and all
controllers/brokers Ready. Initial fault results are not rewritten as if they
used these later resource settings.

An initial CLI verification pass confirmed 11 cases but its authorization test
incorrectly expected removing one cluster Allow to deny all inspection. Other
permissions/API behavior still allowed it. The fixture now uses an explicit
topic Describe Deny for its dedicated principal, restores it afterwards, and
resets fixture ACLs before reruns. Partial verification is not an overall pass.

GitHub CI configuration is included, but no remote repository, push or hosted
CI run has been performed in this task. A local build is not a GitHub CI result.

The final distribution passed all 14 actual CLI checks on 2026-10-15, including
mTLS, invalid CA/hostname rejection, explicit authorization denial, incomplete
configuration and both layouts. [Verification record](VERIFICATION.md).
