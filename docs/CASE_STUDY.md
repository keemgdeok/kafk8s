# Node-loss observations

Status: both layouts completed and their raw evidence reviewed. The two initial
orchestration failures remain excluded. Each layout has one completed run;
repeatability across machines or repeated failures is not yet established.

## Spread run

[Raw evidence](evidence/20261014T143455Z-spread/results.json),
[phases](evidence/20261014T143455Z-spread/phases.json),
[quorum observation](evidence/20261014T143455Z-spread/quorum-during.txt).

RF3, three partitions, ISR [3,7,11], effective min ISR 2, ELR enabled.
Each broker was on a different worker. Worker containing broker 3 was abruptly
stopped for approximately 65 seconds, then restarted. All controllers were on
a different worker.

| Phase | Acknowledged sends | Send errors |
| --- | ---: | ---: |
| Before node-stop request | 63 | 0 |
| Between stop and restart requests | 64 | 1 TimeoutException |
| After restart request | 146 | 0 |

All partitions were observed with ISR [7,11] during the fault and ISR [3,7,11]
after recovery. There were 273 acknowledged event IDs and all were observed.
The unacknowledged/ambiguous timed-out ID was also observed, for 274 unique
observed IDs. Payload checks matched and no repeated ID was observed in this
bounded run. These are not universal no-loss/exactly-once guarantees.

The remaining ISR count met the minimum, yet a transient timeout occurred.
Therefore NO_SHORTFALL must not mean zero interruption or safe drain. The
specific cause of that timeout has not been isolated. Controller leader epochs
changed across earlier observations; the captured during-fault quorum had three
voters, zero follower lag and a leader, not a continuous quorum-availability proof.

## Colocated run

[Raw evidence](evidence/20261014T143858Z-colocated/results.json),
[phase boundaries](evidence/20261014T143858Z-colocated/phases.json).

Broker 7 moved to broker 3's worker. Before the fault all three partitions again
had ISR [3,7,11] and effective min ISR 2. Stopping that worker removed both
brokers. The traffic probe observed ISR [11] repeatedly during the fault, and
ISR [3,7,11] after recovery. The controller worker was not stopped and the three
controller Pods were observed Ready during the fault; continuous quorum health
was not captured for this run.

| Phase | Acknowledged sends | Send errors |
| --- | ---: | ---: |
| Before node-stop request | 66 | 0 |
| Between stop and restart requests | 0 | 14 |
| After restart request | 113 | 5 |

All 19 send errors were TimeoutException. After-restart includes broker startup,
not only the fully recovered state. All 179 acknowledged IDs were observed;
two unacknowledged/ambiguous IDs were also observed, for 181 unique observed IDs.
All observed payloads matched and no repeated ID was observed within the window.

The standalone during-fault metadata probe timed out; its error is retained.
During-fault ISR evidence comes from the already connected traffic probe, not
the empty standalone output. This distinction matters: a fresh diagnostic
connection can fail while an existing client's metadata remains observable.

## Implementation decision after manual comparison

The same three brokers/partitions and min ISR yielded different conditional
budgets: spread leaves 2 observed ISR members, colocated leaves 1. Actual fault
observations matched those counts. The manual workflow required Kafka facts,
four Kubernetes resource reads and an identity join before computing each row.
This justifies a bounded inspector that performs that join and explains inputs;
it does not establish market demand or an actual outage-duration predictor.
See [manual workflow](MANUAL_WORKFLOW.md). No measured time-saving percentage
is claimed. Synthetic traffic and node control remain lab-only functionality.

The implemented CLI was subsequently tested against both live layouts with a
restricted Kubernetes ServiceAccount and mTLS Kafka principal. The final build
passed [43 unit/request tests and 14 CLI checks](VERIFICATION.md). This is a
separate validation step from the original node-loss experiments; resource/auth
settings changed afterwards and the historical raw runs remain unchanged.

## Inconclusive runs

Two earlier runs are not included in the completed comparison:

- [Standalone Admin timeout](evidence/20261014T142753Z-spread/RUN_STATUS.md).
- [Live Bash source-edit race](evidence/20261014T143111Z-spread/RUN_STATUS.md).

Both were restored without stopping existing user services. Their original
observations remain available; partial runs are not relabeled as successes.

## Limits

One local ARM64 host, one Strimzi/Kafka version, low-rate synthetic records,
ephemeral broker storage and a short observation window. Not multi-AZ evidence,
production RTO/SLA, throughput benchmarking or business-pipeline validation.
