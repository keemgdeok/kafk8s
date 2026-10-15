# Inconclusive orchestration run

The first spread run aborted at the standalone during-fault metadata probe:
`TimeoutException: Timed out waiting for a node assignment. Call: listNodes`.
The background traffic probe had observed ISR [7,11] and successful sends.
The EXIT recovery trap restarted the target worker. This run is not counted as
a completed 65-second fault/recovery experiment.

For subsequent runs, the lab probe lists all three broker bootstrap addresses,
uses a bounded 10-second Admin timeout, and retains a standalone observation
failure without skipping recovery. Original evidence remains unchanged.
