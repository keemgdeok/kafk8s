# kafk8s

Explain which Kafka partitions would fall below their minimum ISR after losing
one Kubernetes node, using observed replica/ISR membership and actual Pod placement.

Experimental Java 21 CLI. Currently targets Strimzi 1.3.0, Kafka 4.3.1 KRaft,
metadata.version 30 and ELR version 1. Other profiles return `UNKNOWN`.

## What it answers

> If the brokers on this node disappear together, with no catch-up or recovery,
> which selected partitions would have fewer remaining observed ISR members than
> their effective `min.insync.replicas`?

It assumes producer `acks=all`. It does **not** predict outage duration, data loss,
election success, or whether a drain/rolling update is safe.

## Build and run

With Java 21:

```sh
./gradlew test installDist
build/install/kafk8s/bin/kafk8s --help
```

Without a local JDK, `bash lab/build.sh` uses the pinned Java 21 Docker image.

Run where both the Kubernetes API and **all advertised Kafka broker addresses**
are reachable; forwarding only the bootstrap port is not sufficient.

```sh
build/install/kafk8s/bin/kafk8s check \
  --namespace kafka --cluster my-cluster \
  --bootstrap-server broker-1:9093,broker-2:9093,broker-3:9093 \
  --client-properties client.properties \
  --topic orders --lost-node worker-a --format json
```

Uses the current kubeconfig or an in-cluster ServiceAccount. For an explicit local
selection add `--kubeconfig /path/to/config --context my-context`.
`inspect` shows placement without evaluating a hypothetical loss.
Repeat `--topic` for additional selected topics. No whole-cluster scan.

## Results

| Result | Meaning |
| --- | --- |
| `RISK` | Already below min ISR, or below it under the stated node-loss assumption |
| `NO_SHORTFALL` | No minimum-ISR shortfall found in this calculation; **not SAFE** |
| `UNKNOWN` | Missing, conflicting, changing or unsupported evidence |

Exit codes: `0` no shortfall/complete inspection, `1` risk found, `2` command error,
`3` unknown without known risk. Mixed risk/unknown returns `1` with `complete:false`.
JSON errors are also one document. Timeout defaults to `30s` (maximum `120s`).

The CLI uses Kafka Admin describe requests and namespaced Kubernetes GET/list
requests only. It does not produce/consume records, read Kubernetes Secrets,
delete Pods, reset offsets or change configuration.

## Evidence and limits

[Two node-loss experiments](docs/CASE_STUDY.md) compare spread and colocated
broker layouts. Original failed runs are retained and excluded from completed
comparisons. These are single-host lab observations, not production validation
or proof of demand. External validation is limited.

[Verification](docs/VERIFICATION.md): 43 unit/request tests and 14 actual CLI
checks, including mTLS and permission failures. Hosted CI has not been run.

- [Reproduce the lab](docs/LAB.md)
- [Manual investigation baseline](docs/MANUAL_WORKFLOW.md)
- [Permissions and limitations](docs/LIMITATIONS.md)
- [Implementation checklist](docs/TODO.md)
