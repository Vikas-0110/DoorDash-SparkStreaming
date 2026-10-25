# Milestone 4 — Kafka Offsets and Checkpoint Validation

## Objective

Verify that Spark Structured Streaming resumes processing new Kafka events without reprocessing previously committed records.

## Test Scenario

1. Ingested event `ORD1001` from Kafka into Delta Lake.
2. Published a second event, `ORD1002`.
3. Restarted the streaming query using the existing checkpoint.
4. Verified both orders appeared in the target Delta table.
5. Grouped records by Kafka topic, partition, and offset.

## Actual Results

- Both test events were present in Delta Lake.
- Every Kafka topic/partition/offset combination had a record count of 1.
- No duplicate Kafka records were observed in this test.

## Engineering Learnings

- Kafka offsets identify individual records within partitions.
- Spark checkpoints preserve streaming query progress.
- Reusing the same checkpoint enables processing to resume from recorded progress.
- Starting a query with a new checkpoint may cause historical Kafka records to be read again.

## Limitations

This test validates a normal streaming restart, not recovery from a simulated failure.

## Status

Passed — checkpoint-based restart behavior verified for the tested events.