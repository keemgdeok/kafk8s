# kafk8s

A read-only Java CLI that maps Kafka replicas and ISR members to Kubernetes
nodes and checks for min-ISR shortfalls under a single-node-loss scenario.

Experimental. Currently limited to Strimzi 1.3.0 and Kafka 4.3.1 (KRaft);
see the [supported profile](docs/LAB.md#supported-profile).

## Build and run

Requires Java 21.

```sh
./gradlew test installDist
build/install/kafk8s/bin/kafk8s --help
```

For a Docker-based build, use `bash lab/build.sh`.

The Kubernetes API and all advertised Kafka broker addresses must be reachable.

```sh
build/install/kafk8s/bin/kafk8s check \
  --namespace kafka --cluster my-cluster \
  --bootstrap-server broker-1:9093,broker-2:9093,broker-3:9093 \
  --client-properties client.properties \
  --topic orders --lost-node worker-a --format json
```

Uses the current kubeconfig or an in-cluster ServiceAccount.
`inspect` shows replica placement without evaluating node loss.

## Results

| Result | Meaning |
| --- | --- |
| `RISK` | Below `min.insync.replicas` now or after the specified node loss |
| `NO_SHORTFALL` | No min-ISR shortfall under the stated assumptions; not a safety guarantee |
| `UNKNOWN` | Missing, conflicting, changing or unsupported evidence |

Assumes producer `acks=all`, simultaneous loss of the node's brokers, and no
catch-up or recovery. Results do not predict outage duration, data loss or drain safety.

## Evidence and limits

[Node-loss experiments](docs/CASE_STUDY.md) used RF=3 and min ISR=2, with one
completed run per layout.

| Broker placement | Observed ISR during node loss | Below min ISR |
| --- | ---: | --- |
| One broker per node | 2 | No |
| Two brokers on one node | 1 | Yes |

The spread run still had one producer timeout. These are single-host lab
results, not production validation.

[Verification](docs/CASE_STUDY.md#verification): 43 unit/request tests and 14 actual CLI
checks, including mTLS and permission failures. External validation is limited.

- [Lab setup and permissions](docs/LAB.md)
