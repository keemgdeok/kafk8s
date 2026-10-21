# Node-loss case study

Replica count alone does not describe a partition's node-loss exposure. This
experiment compares two placements with the same RF=3 and min ISR=2.

## Method

Three brokers (IDs 3, 7 and 11), three partitions, ELR enabled, and a producer
with `acks=all` and idempotence enabled. One kind worker was abruptly stopped
for approximately 65 seconds, then restarted. Controllers stayed on a separate
worker to isolate broker-node loss, not to model production controller HA.

Before implementing the CLI, the manual investigation joined Kafka metadata
and configuration with Kafka, KafkaNodePool, Pod and StrimziPodSet resources.
Cluster IDs, owner UIDs and actual node placement identified each broker;
remaining ISR membership was compared with the effective min ISR.

## Results

| Layout | Brokers lost | Predicted / observed ISR count | ACKs during fault | Timeouts during fault |
| --- | --- | --- | ---: | ---: |
| Spread | 3 | 2 / 2 | 64 | 1 |
| Colocated | 3, 7 | 1 / 1 | 0 | 14 |

Raw results: [spread](../lab/evidence/20261014T143455Z-spread/results.json)
and [colocated](../lab/evidence/20261014T143858Z-colocated/results.json).
Both runs returned to ISR [3,7,11] after recovery.

The spread run met min ISR but still had a timeout; its cause was not isolated.
In the colocated run, a fresh metadata probe timed out, so during-fault ISR
evidence comes from the already connected traffic probe, not the empty
standalone output.

All 273 spread-run and 179 colocated-run acknowledged IDs were observed within
the test window. One and two unacknowledged/ambiguous IDs, respectively, were
also observed. No repeated IDs or payload mismatches were observed in that
window; this is not a general losslessness or exactly-once guarantee.

## Design decisions

- Join actual broker placement with observed ISR, rather than counting replicas
  or treating Pod Ready as ISR membership. The CLI automates this correlation;
  no investigation-time savings have been measured.
- Use `NO_SHORTFALL`, not `SAFE`: the calculation does not establish successful
  elections, zero interruption or safe maintenance.
- Return `UNKNOWN` for insufficient, conflicting or changed evidence. Two
  collections detect observed changes but do not form an atomic snapshot.
- Keep synthetic traffic, credentials provisioning and node control in the lab;
  the inspector itself only reads metadata.

## Verification

The 2026-10-15 live CLI verification passed
[14 checks](../lab/evidence/20261015T115647Z-cli/verification.json): both layouts,
mTLS, CA/hostname rejection, permissions, inherited min ISR, JSON errors and
deadlines. The [JAR checksum](../lab/evidence/20261015T115647Z-cli/artifact.sha256)
identifies that tested artifact. These checks are separate from the earlier
node-loss experiments; later auth/resource settings do not rewrite those runs.

The published baseline passed 43 unit/request tests and
[GitHub CI](https://github.com/keemgdeok/kafk8s/actions/runs/39601781337).
CI builds the distribution and checks shell syntax and CLI startup; it does
not run the live-cluster experiments. See [lab reproduction](LAB.md).

## Limits

Each layout has one completed node-loss run on a single ARM64 host, using
low-rate synthetic records and ephemeral storage. This is not a throughput
benchmark, multi-AZ test or production recovery guarantee. Continuous
controller-quorum availability was not established.

Two earlier orchestration attempts (an Admin timeout and a live Bash source-edit
race) were inconclusive. An initial CLI fixture pass also had an incorrect
authorization expectation. None are counted as completed verification; only
the completed fault runs and final CLI verification are kept as reference data.

See [permissions and limitations](LIMITATIONS.md) for the supported profile
and evidence requirements.
