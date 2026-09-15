import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';
import 'package:franz/src/utils/uint8list_native.dart';

import '../../librdkafka/generated_bindings.g.dart';
import '../../librdkafka/loader.dart';

class VariableArguments {
  final Map<int, dynamic> vus;
  VariableArguments() : vus = {};

  int get count => vus.length;

  final List<Pointer> _toFree = [];

  /// Headers object created by [build], if any. librdkafka takes ownership of
  /// it only when `rd_kafka_produceva` succeeds; on failure it stays ours and
  /// must be released with [destroyHeaders].
  Pointer<rd_kafka_headers_t>? _headers;

  void add(rd_kafka_vtype_t vuType, dynamic value) {
    if (vus.containsKey(vuType.value)) {
      throw ArgumentError('Variable argument of type $vuType already exists.');
    }
    vus[vuType.value] = value;
  }

  void remove(int vuType) {
    if (!vus.containsKey(vuType)) {
      throw ArgumentError('Variable argument of type $vuType does not exist.');
    }
    vus.remove(vuType);
  }

  Pointer<rd_kafka_vu_s> build() {
    final count = vus.length;
    // One extra element for the end sentinel (the previous `count * size + 1`
    // allocated a single byte for it and wrote a whole struct there).
    final vusPtr = malloc.allocate<rd_kafka_vu_s>(
      (count + 1) * sizeOf<rd_kafka_vu_s>(),
    );

    int index = 0;
    for (final entry in vus.entries) {
      vusPtr[index].vtypeAsInt = entry.key;
      switch (entry.value) {
        case String value:
          final cstrPtr = value.toNativeUtf8();
          vusPtr[index].u.cstr = cstrPtr.cast<Char>();
          _toFree.add(cstrPtr);
          break;
        case int value:
          vusPtr[index].u.i32 = value;
          break;
        case Uint8List value:
          final nativeData = value.toNative();
          vusPtr[index].u.mem.ptr = nativeData.cast<Void>();
          vusPtr[index].u.mem.size = value.length;
          _toFree.add(nativeData);
          break;
        case Map<String, Uint8List> value
            when entry.key == rd_kafka_vtype_t.RD_KAFKA_VTYPE_HEADERS.value:
          final hdrs = librdkafka.rd_kafka_headers_new(value.length);
          for (final header in value.entries) {
            // rd_kafka_header_add() copies both the name and the value into
            // the header object, so these temporaries are ours to free. They
            // used to be leaked on every produced message.
            final namePtr = header.key.toNativeUtf8();
            final valuePtr = header.value.toNative();
            try {
              librdkafka.rd_kafka_header_add(
                hdrs,
                namePtr.cast<Char>(),
                namePtr.length,
                valuePtr.cast<Void>(),
                header.value.length,
              );
            } finally {
              malloc.free(namePtr);
              malloc.free(valuePtr);
            }
          }
          vusPtr[index].u.headers = hdrs;
          _headers = hdrs;
          break;
        default:
          throw ArgumentError(
            'Unsupported variable argument type: ${entry.value.runtimeType}',
          );
      }
      index++;
    }

    // End sentinel
    vusPtr[index].vtypeAsInt = rd_kafka_vtype_t.RD_KAFKA_VTYPE_END.value;

    return vusPtr;
  }

  /// Releases the headers object built by [build]. Call this only when
  /// `rd_kafka_produceva` failed -- on success librdkafka owns the headers.
  void destroyHeaders() {
    final hdrs = _headers;
    if (hdrs != null) {
      librdkafka.rd_kafka_headers_destroy(hdrs);
      _headers = null;
    }
  }

  void destroy() {
    for (final ptr in _toFree) {
      malloc.free(ptr);
    }
    _toFree.clear();
    _headers = null;
  }
}
