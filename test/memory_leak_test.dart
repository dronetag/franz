// Regression tests for the native memory leaks fixed in 1.0.0.
//
// They run against librdkafka's built-in mock cluster (`test.mock.num.brokers`)
// and therefore need `librdkafka.so` on the library path, but no broker.
import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';
import 'package:franz/franz.dart';
import 'package:franz/librdkafka/generated_bindings.g.dart';
import 'package:franz/librdkafka/loader.dart';
import 'package:test/test.dart';

/// Reads the resident set size of this process in bytes.
int currentRss() => ProcessInfo.currentRss;

/// Configuration that can set arbitrary librdkafka properties on top of the
/// ones [KafkaConfiguration] knows about.
class RawConfiguration extends KafkaConfiguration {
  RawConfiguration(
    this.properties, {
    super.bootstrapServers,
    super.clientId,
    super.groupId,
  });

  final Map<String, String> properties;

  @override
  Pointer<rd_kafka_conf_s> toNative() {
    final conf = super.toNative();
    final errstr = malloc.allocate<Char>(512);
    try {
      for (final entry in properties.entries) {
        final name = entry.key.toNativeUtf8();
        final value = entry.value.toNativeUtf8();
        final result = librdkafka.rd_kafka_conf_set(
          conf,
          name.cast(),
          value.cast(),
          errstr,
          512,
        );
        malloc.free(name);
        malloc.free(value);
        if (result != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK) {
          throw StateError(
            '${entry.key}: ${errstr.cast<Utf8>().toDartString()}',
          );
        }
      }
    } finally {
      malloc.free(errstr);
    }
    return conf;
  }
}

/// An in-process librdkafka mock cluster.
class MockCluster {
  MockCluster._(this._owner, this._cluster, this.bootstrapServers);

  // rdkafka_mock.h is not part of the generated bindings.
  static final _handleMockCluster = dylib.lookupFunction<
    Pointer<Void> Function(Pointer<rd_kafka_t>),
    Pointer<Void> Function(Pointer<rd_kafka_t>)
  >('rd_kafka_handle_mock_cluster');
  static final _bootstraps = dylib.lookupFunction<
    Pointer<Utf8> Function(Pointer<Void>),
    Pointer<Utf8> Function(Pointer<Void>)
  >('rd_kafka_mock_cluster_bootstraps');
  static final _topicCreate = dylib.lookupFunction<
    Int32 Function(Pointer<Void>, Pointer<Utf8>, Int, Int),
    int Function(Pointer<Void>, Pointer<Utf8>, int, int)
  >('rd_kafka_mock_topic_create');

  final Pointer<rd_kafka_t> _owner;
  final Pointer<Void> _cluster;
  final String bootstrapServers;

  static MockCluster start() {
    final errstr = malloc.allocate<Char>(512);
    final conf = RawConfiguration({'test.mock.num.brokers': '1'}).toNative();
    final owner = librdkafka.rd_kafka_new(
      rd_kafka_type_t.RD_KAFKA_PRODUCER,
      conf,
      errstr,
      512,
    );
    if (owner == nullptr) {
      throw StateError(errstr.cast<Utf8>().toDartString());
    }
    malloc.free(errstr);
    final cluster = _handleMockCluster(owner);
    return MockCluster._(owner, cluster, _bootstraps(cluster).toDartString());
  }

  void createTopic(String name, {int partitions = 1}) {
    final namePtr = name.toNativeUtf8();
    final result = _topicCreate(_cluster, namePtr, partitions, 1);
    malloc.free(namePtr);
    if (result != 0) throw StateError('mock topic create failed: $result');
  }

  void dispose() => librdkafka.rd_kafka_destroy(_owner);
}

void main() {
  late MockCluster cluster;

  setUpAll(() {
    try {
      librdkafka.rd_kafka_version();
    } on Object catch (error) {
      markTestSkipped('librdkafka is not available: $error');
      return;
    }
    cluster = MockCluster.start();
  });

  tearDownAll(() => cluster.dispose());

  group('KafkaProducer', () {
    group('producing with headers', () {
      const topicName = 'franz.headers';
      final headers = {
        'acl': ascii.encode(
          List.generate(
            8,
            (i) => 'user:0f7c2a1e-4b6d-4c9a-9e1$i-0a1b2c3d4e5f:ro',
          ).join(','),
        ),
        'uas_id': utf8.encode('1596F0123456789ABCDEF'),
      };

      Future<void> produceBatch(KafkaProducer producer, int count) async {
        var produced = 0;
        while (produced < count) {
          for (var i = 0; i < 2000 && produced < count; i++) {
            try {
              producer.produceStringMessage(
                topic: topicName,
                payload: 'payload',
                headers: headers,
              );
              produced++;
            } on KafkaProduceError {
              // Queue full: let the broker thread drain it.
              await Future<void>.delayed(const Duration(milliseconds: 20));
            }
          }
          await Future<void>.delayed(const Duration(milliseconds: 50));
        }
        await Future<void>.delayed(const Duration(seconds: 1));
      }

      test('does not leak native memory per message', () async {
        cluster.createTopic(topicName);
        final producer = KafkaProducer(
          configuration: RawConfiguration(
            {},
            bootstrapServers: cluster.bootstrapServers,
          ),
        );

        // Warm up: lets librdkafka allocate its queues and buffers so the
        // measured phase reflects steady state only.
        await produceBatch(producer, 100000);
        final before = currentRss();

        const measured = 200000;
        await produceBatch(producer, measured);
        final grownBytes = currentRss() - before;

        // Before the fix this leaked ~400 B per message (headers total
        // ~380 B here), i.e. ~75 MiB for the measured batch.
        expect(
          grownBytes,
          lessThan(20 * 1024 * 1024),
          reason:
              'RSS grew by ${grownBytes ~/ 1024} KiB over $measured messages '
              '(${grownBytes ~/ measured} B/message)',
        );
        producer.dispose();
      }, timeout: const Timeout(Duration(minutes: 3)));
    });
  });

  group('KafkaConsumerTopic', () {
    group('consuming', () {
      test('delivers records with a null key for keyless messages '
          'and never delivers error events as records', () async {
        const topicName = 'franz.consume';
        cluster.createTopic(topicName);

        // enable.partition.eof makes librdkafka return an error event
        // (RD_KAFKA_RESP_ERR__PARTITION_EOF, a *negative* local error) every
        // time the consumer reaches the end of the partition. Those used to
        // be delivered as records whose payload was the error text.
        final consumer = KafkaConsumer(
          configuration: RawConfiguration(
            {'enable.partition.eof': 'true'},
            bootstrapServers: cluster.bootstrapServers,
            groupId: 'franz-test',
          ),
        );
        final producer = KafkaProducer(
          configuration: RawConfiguration(
            {},
            bootstrapServers: cluster.bootstrapServers,
          ),
        );

        final topic = consumer.useTopic(topicName);
        final active = await topic.consumeStart(0, ConsumerOffset.end());
        final received = <ConsumerRecord>[];
        final subscription = active.stream.listen(received.add);

        // Give the consumer time to reach the end of the (empty) partition
        // and receive its first EOF event.
        await Future<void>.delayed(const Duration(seconds: 2));

        for (var i = 0; i < 5; i++) {
          producer.produceStringMessage(topic: topicName, payload: 'msg $i');
        }

        final deadline = DateTime.now().add(const Duration(seconds: 15));
        while (received.length < 5 && DateTime.now().isBefore(deadline)) {
          await Future<void>.delayed(const Duration(milliseconds: 100));
        }
        // Let a trailing EOF event arrive after the last real message.
        await Future<void>.delayed(const Duration(seconds: 2));

        expect(received, hasLength(5));
        for (var i = 0; i < 5; i++) {
          expect(received[i].key, isNull);
          expect(utf8.decode(received[i].payload as List<int>), 'msg $i');
        }

        await subscription.cancel();
        await topic.consumeStop(active);
        producer.dispose();
      }, timeout: const Timeout(Duration(minutes: 1)));
    });
  });
}
