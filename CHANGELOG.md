## 1.0.0

- **Fixed a native memory leak in the producer**: the name and value buffers handed to `rd_kafka_header_add()` (which copies them) were never freed, leaking every header of every produced message
- Fixed the consumer isolate never calling `rd_kafka_message_destroy()` on error events returned by `rd_kafka_consume()`
- Local librdkafka errors (negative `err`, e.g. `_PARTITION_EOF`) are no longer delivered as records carrying the error text as payload
- Consumed records now have `key == null` when the message has no key (was an empty list)
- Headers are destroyed when `rd_kafka_produceva()` fails (ownership stays with the application on failure), and the returned error object is destroyed
- Fixed the size of the `rd_kafka_vu_t` array allocation (the end sentinel used to be written past the allocation)
- Producing defaults to `RD_KAFKA_PARTITION_UA` (partitioner-assigned) instead of partition `0`, matching the 0.3.3 build published to Cloudsmith
- Exceptions (`KafkaProduceError`, ...) are now exported from `package:franz/franz.dart`
- `ConsumerRecord.toString()` no longer throws `RangeError` for binary keys/payloads (bytes were rendered with radix 64)

## 0.3.3

- Expose `ActiveConsumer` class

## 0.3.2

- Added support for a range of librdkafka versions to avoid minor patches causing lib incompatibility

## 0.3.0

- Added support for **headers** in produced messages
    - Uses `rd_kafka_produceva` for producing messages with headers
- Fixed timestamp reading when consuming messages
- Regenerated FFI bindings using `ffigen` version 19.0.0

## 0.2.0

- Updated **support for latest librdkafka 2.8.0**

## 0.1.0

- Initial version with very basic functionalities (simple producing & consuming)
