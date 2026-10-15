# Verification — 2026-10-15

Scope: the final local Java 21 distribution, Strimzi 1.3.0/Kafka 4.3.1 KRaft,
kind Kubernetes 1.35.8 on a single macOS ARM64 host. Java ran in Linux containers.
This is not a public release, hosted CI result or production certification.

## Evidence

| Check | Result | Evidence |
| --- | --- | --- |
| Distributed broker-node loss and recovery | Completed | [Spread run](evidence/20261014T143455Z-spread/results.json) |
| Two colocated brokers lost together and recovered | Completed | [Colocated run](evidence/20261014T143858Z-colocated/results.json) |
| Manual correlation versus proposed CLI | Reviewed before implementation | [Manual workflow](MANUAL_WORKFLOW.md), [decision](CASE_STUDY.md) |
| Final unit/request tests | 43 passed, no failures/errors | [Reports](evidence/20261015T115647Z-cli/) |
| Final actual CLI checks | 14 passed, exit codes and JSON predicates verified | [Machine-readable results](evidence/20261015T115647Z-cli/verification.json) |
| Final application JAR checksum | Verified with shasum -c | [SHA256](evidence/20261015T115647Z-cli/artifact.sha256) |
| Workflow syntax | actionlint 1.7.7 passed | `.github/workflows/ci.yml` |

## Actual CLI cases

1. mTLS inspection with the restricted ServiceAccount/principal.
2. TLS hostname mismatch rejected.
3. Wrong CA rejected.
4. Colocated node loss gives RISK, remaining ISR 1, exit 1.
5. Unobserved node argument gives structured ERROR, exit 2.
6. Missing required arguments give one JSON error document.
7. Topic configuration denied gives UNKNOWN, exit 3.
8. Mixed RISK/UNKNOWN retains both with complete:false, exit 1.
9. Inherited min ISR without broker describe-config access gives UNKNOWN.
10. Inherited min ISR with access verifies consistent broker defaults.
11. An authorized but nonexistent topic gives TOPIC_NOT_FOUND.
12. An explicit topic Describe Deny gives AUTHORIZATION_ERROR.
13. Spread layout gives NO_SHORTFALL, remaining ISR 2, exit 0.
14. Unreachable Kafka with a two-second collection budget gives a JSON deadline error.

The Kubernetes reader had no Secret access. Request tests prove forbidden
resource paths are rejected before transmission and allowed reads use GET with
the selected cluster label. Kafka production code contains Admin describe calls,
not a producer/consumer or mutation API. The lab administrator—not the CLI—
generated credentials, copied files, changed ACLs and induced faults.

The original failed orchestration runs and a partial CLI fixture run remain in
the evidence tree. They are not counted as completed verification. Each layout
has only one completed node-loss experiment; CLI layout checks do not count as
additional node-loss repetitions. No production RTO, losslessness or market
demand is inferred from these results.

Final lab state: separate broker placement restored, full ISR verified, explicit
test Deny removed. Existing Airflow/PostgreSQL services were not stopped.
The lab remains running for inspection. Generated credentials/kubeconfig are
gitignored. No remote repository creation, push, PR, merge or release was performed.
