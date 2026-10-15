# Inconclusive orchestration run

The running Bash script was edited to add a safety check while Bash was still
reading subsequent commands. It failed with an unexpected EOF. The file now
passes `bash -n`; this was an execution/source-edit race, not a Kafka failure.
The EXIT trap restarted the target worker, and the bounded traffic probe remained
live until its own deadline. This run is excluded from completed comparisons.

`lab/run.sh` now validates and reads the whole experiment script before executing
it, so an edit cannot shift the running script's read position. Future failed
orchestration runs retain an explicit INCONCLUSIVE status and wait for the
bounded traffic probe after node recovery.
