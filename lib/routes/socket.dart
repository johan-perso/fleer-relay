import 'package:fleer_backend/routes/shares.dart';
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

  bool isConnectedToShare = false;

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

  // Clean connections that are not connected to a share and that are inactive
  void cleanInactiveConnections() {
    Duration maxAge = const Duration(minutes: 1);
    final now = DateTime.now().toUtc();

    final inactiveConnections = _connections.values.where((c) => !c.isConnectedToShare && now.difference(c.connectedAt) > maxAge).toList();
    for (final connection in inactiveConnections) {
      connection.send('error', {'error': 'inactive', 'message': 'Connection closed due to inactivity'});
      connection.close(code: ws_status.normalClosure, reason: 'inactive');
      _connections.remove(connection.id);
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
  'ConnectToShare': _handleConnectToShare,
};

void _handleConnectToShare(SocketConnection connection, Object? data) {
  if (connection.isConnectedToShare) {
    connection.send('error', {'error': 'already_connected', 'message': 'You are already connected to a share'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'already_connected'));
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('error', {'error': 'invalid_data', 'message': 'Invalid data format for ConnectToShare'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  final shareId = data['shareId']?.toString();
  if (shareId == null || shareId.isEmpty) {
    connection.send('error', {'error': 'missing_shareId', 'message': 'Missing or empty shareId'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_shareId'));
    return;
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    connection.send('error', {'error': 'share_not_found', 'message': 'Share not found for shareId: $shareId'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_not_found'));
    return;
  }

  final isSender = data['isSender'] == true;
  if (isSender) {
    if (share.senderConnection != null) {
      connection.send('error', {'error': 'sender_already_connected', 'message': 'A sender is already connected to this share'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'sender_already_connected'));
      return;
    }

    share.senderConnection = connection;
  }

  if (!isSender) { // then, we are a receiver
    if (share.receiverConnection != null) {
      connection.send('error', {'error': 'receiver_already_connected', 'message': 'A receiver is already connected to this share'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'receiver_already_connected'));
      return;
    }

    final deviceName = data['deviceName']?.toString();
    if (deviceName == null || deviceName.isEmpty) {
      connection.send('error', {'error': 'missing_deviceName', 'message': 'Missing or empty deviceName'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_deviceName'));
      return;
    }

    if (deviceName.length > globals.maxDeviceNameLength) {
      connection.send('error', {'error': 'deviceName_too_long', 'message': 'Device name exceeds maximum length of ${globals.maxDeviceNameLength} characters'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'deviceName_too_long'));
      return;
    }

    share.receiverDeviceName = deviceName;
    share.receiverConnection = connection;
  }

  share.touch();
  connection.isConnectedToShare = true;
  connection.send('connectedToShare', {'shareId': shareId, 'isSender': isSender});
}

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