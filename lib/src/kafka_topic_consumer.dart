part of 'kafka_topic.dart';

const timeoutMs = 1000; // FIXME: make configurable

class KafkaConsumerTopic extends KafkaTopic {
  final StreamController<ConsumerRecord> _sink =
      StreamController<ConsumerRecord>.broadcast();

  KafkaConsumerTopic({required super.kafkaNativeInstance, required super.name});

  Stream<ConsumerRecord> get stream => _sink.stream;

  Future<ActiveConsumer> consumeStart(
    int partition,
    ConsumerOffset offset,
  ) async {
    librdkafka.rd_kafka_consume_start(
      _$native,
      partition,
      offset.numericOffset,
    );

    final messagesSink = ReceivePort();
    final isolate = await Isolate.spawn(
      (sendPort) => _consumerIsolate(partition, sendPort),
      messagesSink.sendPort,
      debugName: "franz-consumer-$name-$partition",
    );

    return ActiveConsumer(
      messagesSink.asBroadcastStream().cast<ConsumerRecord>(),
      partition,
      isolate,
    );
  }

  Future<void> consumeStop(ActiveConsumer activeConsumer) async {
    librdkafka.rd_kafka_consume_stop(_$native, activeConsumer.partition);
    activeConsumer.cancel();
  }

  void _consumerIsolate(int partition, SendPort messagesOutlet) {
    // - This method loops in isolate
    while (true) {
      final message = librdkafka.rd_kafka_consume(
        _$native,
        partition,
        timeoutMs,
      );

      if (message == nullptr) continue;

      // Every non-NULL message returned by rd_kafka_consume() -- including
      // error events -- must be released with rd_kafka_message_destroy().
      try {
        _handleMessage(message, partition, messagesOutlet);
      } finally {
        librdkafka.rd_kafka_message_destroy(message);
      }
    }
  }

  void _handleMessage(
    Pointer<rd_kafka_message_t> message,
    int partition,
    SendPort messagesOutlet,
  ) {
    // Local (librdkafka-internal) errors are negative, broker errors are
    // positive; only NO_ERROR (0) carries an actual record. The previous
    // `err > 0` check treated local errors as records whose payload was the
    // error string.
    if (message.ref.err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR) {
      if (message.ref.err ==
          rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        print(
          "!! Reached end of topic $name [${message.ref.partition}] at offset ${message.ref.offset}",
        );
      } else {
        final errorCstr =
            librdkafka.rd_kafka_message_errstr(message).cast<Utf8>();
        print("Consumption error: ${errorCstr.toDartString()}");
      }
      return;
    }

    final timestampValue = librdkafka.rd_kafka_message_timestamp(
      message,
      nullptr,
    );

    // Copy key and payload out of librdkafka's buffers: the views created by
    // asTypedList() would dangle once the message is destroyed.
    final key =
        message.ref.key == nullptr
            ? null
            : Uint8List.fromList(
              message.ref.key.cast<Uint8>().asTypedList(message.ref.key_len),
            );
    final payload =
        message.ref.payload == nullptr
            ? null
            : Uint8List.fromList(
              message.ref.payload.cast<Uint8>().asTypedList(message.ref.len),
            );

    final consumerRecord = ConsumerRecord(
      topic: name,
      partition: partition,
      offset: message.ref.offset,
      timestamp:
          timestampValue == -1
              ? DateTime.now()
              : DateTime.fromMillisecondsSinceEpoch(timestampValue),
      key: key,
      payload: payload,
    );
    messagesOutlet.send(consumerRecord);
  }
}
