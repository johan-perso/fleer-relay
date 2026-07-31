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
  String? connectedShareId;

  bool isDownloadResumed = false;

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
  void remove(SocketConnection connection) {
    ShareDetails? share = sharedDetails[connection.connectedShareId];
    if (share != null) {
      if (share.senderConnection == connection) {
        share.senderConnection = null;
      }
      if (share.receiverConnection == connection) {
        share.receiverConnection = null;
      }
    }

    _connections.remove(connection.id);
  }

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
      connection.send('fatal', {'error': 'inactive', 'message': 'Connection closed due to inactivity'});
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
  'GetPrecedentsChunks': _handleSendingPrecedentChunks,
  'ResumeDownloading': _handleResumeDownloading,
  'AcknowledgeChunks': _handleAcknowledgeChunks,
};

void _handleConnectToShare(SocketConnection connection, Object? data) {
  if (connection.isConnectedToShare) {
    connection.send('error', {'error': 'already_connected', 'message': 'You are already connected to a share'});
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('fatal', {'error': 'invalid_data', 'message': 'Invalid data format for ConnectToShare'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  final shareId = data['shareId']?.toString();
  if (shareId == null || shareId.isEmpty) {
    connection.send('fatal', {'error': 'missing_shareId', 'message': 'Missing or empty shareId'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_shareId'));
    return;
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    connection.send('fatal', {'error': 'share_not_found', 'message': 'Share not found for shareId: $shareId'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_not_found'));
    return;
  }

  final isSender = data['isSender'] == true;
  if (isSender) {
    if (share.senderConnection != null) {
      connection.send('fatal', {'error': 'sender_already_connected', 'message': 'A sender is already connected to this share'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'sender_already_connected'));
      return;
    }

    share.senderConnection = connection;

    if (share.receiverDeviceName != null) {
      connection.send('receiverName', {'name': share.receiverDeviceName});
    }
  }

  if (!isSender) { // then, we are a receiver
    if (share.receiverConnection != null) {
      connection.send('fatal', {'error': 'receiver_already_connected', 'message': 'A receiver is already connected to this share'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'receiver_already_connected'));
      return;
    }

    final deviceName = data['deviceName']?.toString();
    if (deviceName == null || deviceName.isEmpty) {
      connection.send('fatal', {'error': 'missing_deviceName', 'message': 'Missing or empty deviceName'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_deviceName'));
      return;
    }

    if (deviceName.length > globals.maxDeviceNameLength) {
      connection.send('fatal', {'error': 'deviceName_too_long', 'message': 'Device name exceeds maximum length of ${globals.maxDeviceNameLength} characters'});
      unawaited(connection.close(code: ws_status.normalClosure, reason: 'deviceName_too_long'));
      return;
    }

    share.receiverDeviceName = deviceName;
    share.receiverConnection = connection;

    if (share.senderConnection != null) {
      share.senderConnection!.send('receiverName', {'name': deviceName});
    }
  }

  share.touch();
  connection.connectedShareId = shareId;
  connection.isConnectedToShare = true;
  connection.send('connectedToShare', {'shareId': shareId, 'isSender': isSender});
}

void _handleSendingPrecedentChunks(SocketConnection connection, Object? data) {
  if (connection.isDownloadResumed) {
    connection.send('fatal', {'error': 'download_has_resumed', 'message': 'You cannot request precedent chunks if download is resumed'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'download_has_resumed'));
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('fatal', {'error': 'invalid_data', 'message': 'Invalid data format for GetPrecedentsChunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  String? shareId = connection.connectedShareId;
  if (!connection.isConnectedToShare || shareId == null || shareId.isEmpty) {
    connection.send('fatal', {'error': 'not_connected_to_share', 'message': 'You are not connected to any share'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'not_connected_to_share'));
    return;
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    connection.send('fatal', {'error': 'share_deleted', 'message': 'The share you were connected to no longer exists'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_deleted'));
    return;
  }

  final isSender = share.senderConnection == connection;
  if (isSender) {
    connection.send('fatal', {'error': 'sender_cannot_request_chunks', 'message': 'Senders cannot request precedent chunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'sender_cannot_request_chunks'));
    return;
  }

  int fromChunkId = 0;
  if (data['fromChunkId'] is int) {
    fromChunkId = data['fromChunkId'] as int;
  } else if (data['fromChunkId'] is String) {
    fromChunkId = int.tryParse(data['fromChunkId'] as String) ?? 0;
  }

  int untilChunkId = 0;
  if (data['untilChunkId'] is int) {
    untilChunkId = data['untilChunkId'] as int;
  } else if (data['untilChunkId'] is String) {
    untilChunkId = int.tryParse(data['untilChunkId'] as String) ?? 0;
  }

  if (untilChunkId > 0 && untilChunkId < fromChunkId) {
    connection.send('fatal', {'error': 'invalid_chunk_range', 'message': 'untilChunkId must be greater than or equal to fromChunkId'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_chunk_range'));
    return;
  }
  if (untilChunkId > 0 && untilChunkId > share.chunks.length) {
    untilChunkId = share.chunks.length; // silently adjust untilChunkId to the maximum available chunk index
  }

  // Send chunks that the receiver has not received yet, but that we already got from the sender
  for (int i = fromChunkId; i < (untilChunkId > 0 ? untilChunkId : share.chunks.length); i++) {
    final chunkData = share.chunks[i.toString()];
    if (chunkData != null) {
      share.chunksSentToReceiver[i.toString()] = true;
      connection.sendBinary(frameChunk(i, chunkData));
    }
  }

  share.touch();
}

void _handleResumeDownloading(SocketConnection connection, Object? data) {
  if (connection.isDownloadResumed) {
    connection.send('error', {'error': 'already_resumed', 'message': 'You have already resumed downloading'});
    return;
  }

  String? shareId = connection.connectedShareId;
  if (!connection.isConnectedToShare || shareId == null || shareId.isEmpty) {
    connection.send('fatal', {'error': 'not_connected_to_share', 'message': 'You are not connected to any share'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'not_connected_to_share'));
    return;
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    connection.send('fatal', {'error': 'share_deleted', 'message': 'The share you were connected to no longer exists'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_deleted'));
    return;
  }

  final isSender = share.senderConnection == connection;
  if (isSender) {
    connection.send('fatal', {'error': 'sender_cannot_resume', 'message': 'Senders cannot resume downloading'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'sender_cannot_resume'));
    return;
  }

  if (share.chunks.isNotEmpty && share.chunksSentToReceiver.entries.any((entry) => entry.value != true || (entry.value == true && share.chunksAcknowledgedByReceiver[entry.key] != true))) {
    connection.send('fatal', {'error': 'incomplete_chunks', 'message': 'You cannot resume downloading if there are chunks that have been sent to you but not yet acknowledged'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'incomplete_chunks'));
    return;
  }

  connection.isDownloadResumed = true;
  share.touch();
}

void _handleAcknowledgeChunks(SocketConnection connection, Object? data) {
  String? shareId = connection.connectedShareId;
  if (!connection.isConnectedToShare || shareId == null || shareId.isEmpty) {
    connection.send('fatal', {'error': 'not_connected_to_share', 'message': 'You are not connected to any share'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'not_connected_to_share'));
    return;
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    connection.send('fatal', {'error': 'share_deleted', 'message': 'The share you were connected to no longer exists'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_deleted'));
    return;
  }

  final isSender = share.senderConnection == connection;
  if (isSender) {
    connection.send('fatal', {'error': 'sender_cannot_acknowledge_chunks', 'message': 'Senders cannot acknowledge chunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'sender_cannot_acknowledge_chunks'));
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('fatal', {'error': 'invalid_data', 'message': 'Invalid data format for AcknowledgeChunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  List<dynamic>? acknowledgedChunks = data['acknowledgedChunks'] as List<dynamic>?;
  if (acknowledgedChunks == null || acknowledgedChunks.isEmpty) {
    connection.send('error', {'error': 'missing_acknowledgedChunks', 'message': 'Missing or empty acknowledgedChunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_acknowledgedChunks'));
    return;
  } else {
    for (final dynamic chunkId in acknowledgedChunks) {
      String chunkIdStr = chunkId is String ? chunkId : chunkId.toString();

      if (!share.chunksSentToReceiver.containsKey(chunkIdStr)) {
        connection.send('fatal', {'error': 'invalid_chunkId', 'message': 'Chunk ID $chunkIdStr was not sent to you'});
        unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_chunkId'));
        continue;
      }

      share.chunksAcknowledgedByReceiver[chunkIdStr] = true;
    }
  }

  // Allow the sender to send more chunks to the receiver, based on what the receiver has acknowledged
  // Example: the receiver has acknowledged 40 MB of chunks, so the sender can send 40 MB more to the receiver
  share.allowedBytesMax = share.allowedBytesMax + acknowledgedChunks.fold<int>(0, (sum, chunkId) {
    String chunkIdStr = chunkId is String ? chunkId : chunkId.toString();
    final chunkData = share.chunks[chunkIdStr];
    return sum + (chunkData?.length ?? 0);
  });

  // Check if there is any pending chunks that the receiver has not acknowledged yet, and if so, send them to the receiver again
  final pendingChunks = share.chunksSentToReceiver.entries.where((entry) => entry.value == true && share.chunksAcknowledgedByReceiver[entry.key] != true).toList();
  if (pendingChunks.isNotEmpty) {
    // Only send a few pending chunks to avoid overwhelming the receiver
    // Less chunks the receiver has acknowledged, more chunks we send to the receiver
    int unacknowledgedChunksAmount = pendingChunks.length;
    for (var i = 0; i < pendingChunks.length; i++) {
      if (i >= (3 - unacknowledgedChunksAmount)) break; // send only a few pending chunks to avoid overwhelming the receiver

      final entry = pendingChunks[i];
      final chunkIdStr = entry.key;
      final chunkData = share.chunks[chunkIdStr];
      if (chunkData != null) {
        connection.sendBinary(frameChunk(int.tryParse(chunkIdStr) ?? 0, chunkData));
      }
      pendingChunks.remove(entry); // remove the chunk from the pending list after sending it
    }
  }

  // If there is no pending chunks, allow the sender to send more chunks to the receiver
  if (pendingChunks.isEmpty) share.canSendChunksToReceiver = true;
  share.touch();
  share.clearAcknowledgedChunks();
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
      'connectionId': connection.id
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