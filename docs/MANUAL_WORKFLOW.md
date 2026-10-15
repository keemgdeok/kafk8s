# Manual investigation baseline

This is the workflow actually used in the disposable lab. No production time
savings or adoption claims are made.

1. Read Kafka topic metadata and effective topic configuration using the official
   Admin client (`lab.Probe metadata`), including cluster ID and finalized features.
2. Read `Kafka/demo`: cluster ID, observed generation, Kafka/Operator version, UID.
3. List the selected cluster's KafkaNodePools: actual nodeIds, roles, cluster ID,
   generation and pool UID. Do not infer broker IDs from list order or pod ordinal.
4. List Pods: actual nodeName, UID, role/pool/cluster labels and PodSet owner UID.
5. List StrimziPodSets: verify each selected PodSet belongs to the matching pool UID.
6. For each selected partition, join ISR broker IDs to actual nodes and remove only
   ISR members on the proposed lost node; compare the remaining count to the
   effective min ISR, not just the replica count or cluster-wide spread.

The first five operations collect facts; the sixth is a cross-system join and
conditional calculation. Repeated snapshots are necessary because neither
system supplies an atomic Kafka-plus-Kubernetes snapshot.

The scripts preserve before/during/after results. The manual baseline involves
one Kafka metadata/configuration probe and four Kubernetes resource reads plus
human correlation. This is a count of top-level investigation steps, not five
network requests, nor a measured speedup. The probe itself uses multiple Admin
requests. Controller quorum was also checked separately to validate the lab.

## Concrete manual cross-check

Both baselines had ISR [3,7,11] and min ISR 2. The lost node was
`kafk8s-lab-worker`; broker 11 stayed on `kafk8s-lab-worker3`.

| Layout | Brokers on lost node | ISR left by manual set subtraction | Compare to min ISR 2 | Actual during-fault ISR |
| --- | --- | --- | --- | --- |
| Spread | 3 | [7,11], count 2 | No shortfall in this model | [7,11] |
| Colocated | 3,7 | [11], count 1 | Below minimum | [11] |

Inputs are preserved in each run's before-pools/pods/podsets and
before-metadata files. See [spread evidence](evidence/20261014T143455Z-spread/)
and [colocated evidence](evidence/20261014T143858Z-colocated/). In the latter,
the traffic probe supplies during-fault ISR because the fresh standalone
metadata probe failed. This comparison preceded product implementation.

## What would justify a small CLI

The candidate should automate the join and show the specific partition, lost
ISR members, remaining members, effective threshold and identity evidence.
Reprinting the same five raw documents would not justify another tool.

Fact collection must remain separate from the availability claim. A remaining
ISR count at/above min ISR is not a guarantee of zero transient produce errors,
successful election, safe drain, end-to-end processing or data durability.
