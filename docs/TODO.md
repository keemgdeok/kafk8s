# Implementation and verification

Work branch: `feat/replica-risk-lab`. No publication or release is requested.

- [x] Create an isolated kind/Strimzi/KRaft lab without changing existing services.
- [x] Verify cluster identity, broker/Pod/node mapping, effective min ISR and ELR.
- [x] Record healthy spread layout, abrupt broker-node loss and recovery.
- [x] Repeat with two brokers on one node, without stopping the controller node.
- [x] Preserve raw observations and distinguish transient errors from ISR shortfall.
- [x] Compare the manual investigation workflow before choosing the CLI scope.
- [x] Implement selected-topic inspection and conditional node-loss analysis only.
- [x] Test missing/conflicting evidence, real configuration, deadlines and output.
- [x] Compare the CLI with real lab observations, including limited permissions.
- [x] Document measured results, reproducibility and known limitations.

Final evidence: [43 unit/request tests and 14 real CLI checks](VERIFICATION.md).
Hosted CI, publication and production validation are not completion claims.

The tool must not claim drain safety, no data loss, exactly-once processing or
production recovery guarantees. Sending/consuming records and stopping nodes
belong to the explicitly isolated lab, not to the inspector.
