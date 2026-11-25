These runs are NOT valid results; kept only for the record.

experiment-Zookeeper-20261113-164318.json: the laptop slept mid-run. Training was cut off and its leftover LLM calls leaked into validation/test; Kafka rejected messages after wake-up (InvalidTimestampError).
experiment-Linux-20261113-174403.json: measurement bug (fixed): results were read before Kafka had delivered every line, so test counted 245 of 400 lines.
