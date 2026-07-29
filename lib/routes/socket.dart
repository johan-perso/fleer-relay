import 'package:fleer_backend/utils/globals.dart' as globals;

import 'dart:async';
import 'dart:convert';
import 'package:nanoid2/nanoid2.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_web_socket/shelf_web_socket.dart';
import 'package:web_socket_channel/status.dart' as ws_status;
import 'package:web_socket_channel/web_socket_channel.dart';

class SocketMessage {
  const SocketMessage(this.type, [this.data]);

  final String type;
  final Object? data;

  String encode() => jsonEncode({
    'type': type,
    if (data != null) 'data': data,
  });

  static SocketMessage? tryParse(String raw) {
    final Object? decoded;
    try {
      decoded = jsonDecode(raw);
    } on FormatException {
      return null;
    }

    if (decoded is! Map<String, Object?>) return null;

    final type = decoded['type'];
    if (type is! String || type.isEmpty) return null;

    return SocketMessage(type, decoded['data']);
  }
}

class SocketConnection {
  SocketConnection(this._channel) : id = nanoid(length: 10),
    connectedAt = DateTime.now().toUtc();

  final WebSocketChannel _channel;
  final String id;
  final DateTime connectedAt;

  bool _closed = false;

  // Method to encode and send a JSON message to the client
  void send(String type, [Object? data]) {
    if (_closed) return;
    _channel.sink.add(SocketMessage(type, data).encode());
  }

  // Method to send an already JSON encoded frame to the client
  void sendRaw(String frame) {
    if (_closed) return;
    _channel.sink.add(frame);
  }

  // Method to send a binary frame to the client
  void sendBinary(List<int> bytes) {
    if (_closed) return;
    _channel.sink.add(bytes);
  }

  // Method to close the connection with an optional code and reason
  Future<void> close({
    int code = ws_status.normalClosure,
    String? reason,
  }) async {
    if (_closed) return;
    _closed = true;
    await _channel.sink.close(code, reason);
  }
}

class SocketRegistry {
  final Map<String, SocketConnection> _connections = {};

  // Add or remove a connection from the registry
  void add(SocketConnection connection) => _connections[connection.id] = connection;
  void remove(SocketConnection connection) => _connections.remove(connection.id);

  // Broadcast a message to all connections, optionally excluding one
  void broadcast(String type, [Object? data, SocketConnection? except]) {
    final frame = SocketMessage(type, data).encode();
    for (final connection in _connections.values) {
      if (connection != except) connection.sendRaw(frame);
    }
  }
}

final socketRegistry = SocketRegistry();
typedef SocketHandler = FutureOr<void> Function(
  SocketConnection connection,
  Object? data,
);

final Map<String, SocketHandler> _handlers = {
  'FleerPing': (connection, data) => connection.send('FleerPong', data),
};

Handler socketHandler() => webSocketHandler(
  (WebSocketChannel channel, String? subprotocol) => _onConnect(channel),
  pingInterval: const Duration(seconds: 30)
);

void _onConnect(WebSocketChannel channel) {
  final connection = SocketConnection(channel);
  socketRegistry.add(connection);

  channel.stream.listen(
    (raw) => _onMessage(connection, raw),
    onDone: () => socketRegistry.remove(connection),
    onError: (Object _) => socketRegistry.remove(connection),
    cancelOnError: true,
  );

  connection.send(
    'welcome',
    {
      'connectionId': connection.id,
      'protocolVersion': globals.protocolVersion
    }
  );
}

void _onMessage(SocketConnection connection, Object? raw) {
  // A WebSocket frame can be either a String (text) or a List<int> (binary), we need to handle both cases
  // and optionally decode the binary data as UTF-8 if needed
  final text = switch (raw) {
    String value => value,
    List<int> bytes => utf8.decode(bytes, allowMalformed: true),
    _ => null,
  };

  if (text == null) {
    connection.send('error', {'message': 'Unsupported frame type'});
    return;
  }

  final message = SocketMessage.tryParse(text);
  if (message == null) {
    connection.send('error', {
      'message': 'Expected a JSON object with a non-empty "type" field',
    });
    return;
  }

  final handler = _handlers[message.type];
  if (handler == null) {
    connection.send('error', {'message': 'Unknown type: ${message.type}'});
    return;
  }

  handler(connection, message.data);
}