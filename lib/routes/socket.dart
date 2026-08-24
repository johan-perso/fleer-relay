import 'package:fleer_relay/routes/shares.dart';
import 'package:fleer_relay/utils/globals.dart' as globals;

import 'dart:async';
import 'dart:convert';
import 'dart:math';
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
  ShareRole? shareRole;

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
  void remove(SocketConnection connection, { String? reason }) {
    ShareDetails? share = sharedDetails[connection.connectedShareId];
    if (share != null) {
      if (share.senderConnection == connection) {
        share.senderConnection = null;
      }
      if (share.receiverConnection == connection) {
        share.receiverConnection = null;
        share.canSendChunksToReceiver = false;
        share.chunksSentToReceiver.clear();
        share.chunksAcknowledgedByReceiver.clear();
      }

      // share.touch(); // not needed because a deconnection should not lock the share from being auto deleted for another 5 minutes
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
  'AcknowledgeChunks': _handleAcknowledgeChunks,
  'SendMsgToOtherWay': _handleSengMsgToOtherWay,
  'LastChunk': _handleLastChunkIndication,
  'DeleteTransfer': _handleTransferDeletion,
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
    connection.shareRole = ShareRole.sender;

    if (share.receiverDeviceName != null) {
      connection.send('receiverName', {'name': share.receiverDeviceName});
    }

    for (final message in share.messagesFromReceiverQueue) {
      connection.send('msgFromReceiver', message);
    }

    if (share.receiverConnection != null) {
      share.receiverConnection!.send('senderStatus', {'connected': true, 'message': 'A sender has/is connected to the share'});
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
    connection.shareRole = ShareRole.receiver;

    if (share.senderConnection != null) {
      share.senderConnection!.send('receiverName', {'name': deviceName});
      share.receiverConnection!.send('receiverStatus', {'connected': true, 'message': 'A receiver has/is connected to the share'});
    }

    if (share.lastChunkId != null) {
      connection.send('lastChunkIndicated', {'lastChunkId': share.lastChunkId, 'message': 'The sender has indicated the last chunk to be sent'});
    }

    for (final message in share.messagesFromSenderQueue) {
      connection.send('msgFromSender', message);
    }
  }

  share.touch();
  connection.connectedShareId = shareId;
  connection.isConnectedToShare = true;
  connection.send(
    'connectedToShare',
    {
      'shareId': shareId,
      'isSenderConnected': share.senderConnection != null,
      'isReceiverConnected': share.receiverConnection != null,
      'message': 'Successfully connected to share, you will receive new chunks and messages from the other side',
    });
}

void _handleSendingPrecedentChunks(SocketConnection connection, Object? data) {
  if (connection.isDownloadResumed) {
    connection.send('fatal', {'error': 'download_has_resumed', 'message': 'You cannot request precedent chunks if download has already resumed'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'download_has_resumed'));
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('fatal', {'error': 'invalid_data', 'message': 'Invalid data format for GetPrecedentsChunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  ShareDetails? share = _checkSocketConnectedShare(connection);
  if(share == null) return;

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

  if (share.chunks.isNotEmpty) {
    int highestChunkId = share.chunks.keys.last;
    if (untilChunkId > highestChunkId) {
      // Silently adjust untilChunkId to the maximum available chunk index if it exceeds the number of chunks we have
      untilChunkId = highestChunkId;
    }
  }

  // Send chunks that the receiver has not received yet, but that we already got from the sender
  for (int i = fromChunkId; i <= untilChunkId; i++) {
    if (i > untilChunkId) break;

    final chunkData = share.chunks[i];
    if (share.chunks.containsKey(i) && chunkData == null) {
      // This mean we already received this chunk from the sender, but we cleared it from the memory afterwards.
      // Because the receiver need it again, we need to make the sender resend it to us.
      share.restartTransfer();
      return;
    }

    if (chunkData != null) {
      share.chunksSentToReceiver[i] = true;
      connection.sendBinary(frameChunk(i, chunkData));
    }
  }

  // If there is no pending to be sent chunks anymore
  List pendingChunks = share.chunks.entries.where((entry) => share.chunksSentToReceiver[entry.key] != true).toList();
  bool isTherePendingChunks = pendingChunks.isNotEmpty;
  if (!isTherePendingChunks) {
    connection.send('precedentsChunksUpdate', {'remaining': 0, 'message': 'All chunks have been sent to the receiver'});
    connection.isDownloadResumed = true; // there is no chunk to acknowledge, so we need to resume the download right here
  } else {
    connection.send('precedentsChunksUpdate', {'remaining': pendingChunks.length, 'message': 'There are still chunks that have not been sent to the receiver'});
    // no need to enable share.canSendChunksToReceiver here, because it will be enabled when the receiver acknowledges the chunks
  }

  share.touch();
}

void _handleAcknowledgeChunks(SocketConnection connection, Object? data) {
  ShareDetails? share = _checkSocketConnectedShare(connection);
  if(share == null) return;

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

  List<dynamic>? acknowledgedChunks = [];
  List<dynamic>? acknowledgedChunksUnparsed = data['chunkIds'] as List<dynamic>?;
  if (acknowledgedChunksUnparsed == null || acknowledgedChunksUnparsed.isEmpty) {
    connection.send('error', {'error': 'missing_acknowledgedChunks', 'message': 'Missing or empty acknowledgedChunks'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'missing_acknowledgedChunks'));
    return;
  } else {
    for (final dynamic chunkIdUnparsed in acknowledgedChunksUnparsed) {
      int? chunkId = chunkIdUnparsed is int ? chunkIdUnparsed : int.tryParse(chunkIdUnparsed.toString());
      if (chunkId == null) {
        connection.send('fatal', {'error': 'invalid_chunkId', 'message': 'Invalid chunkId: $chunkIdUnparsed'});
        unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_chunkId'));
        return;
      } else {
        acknowledgedChunks.add(chunkId);
      }

      if (!share.chunksSentToReceiver.containsKey(chunkId)) {
        connection.send('fatal', {'error': 'invalid_chunkId', 'message': 'Chunk ID $chunkId was not sent to you, or has already been acknowledged'});
        unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_chunkId'));
        continue;
      }

      share.chunksAcknowledgedByReceiver[chunkId] = true;
    }
  }

  // Allow the sender to send us more chunks, based on what the receiver has acknowledged
  // Example: the receiver has acknowledged 40 MB of chunks, so the sender can send 40 MB more to the receiver
  share.allowedBytesMax = share.allowedBytesMax + acknowledgedChunks.fold<int>(0, (sum, chunkId) {
    final chunkData = share.chunks[chunkId];
    return sum + (chunkData?.length ?? 0);
  });
  share.senderConnection?.send('allowedBytesMaxUpdate', {'allowedBytesMax': share.allowedBytesMax, 'message': 'The receiver has acknowledged chunks, you can send more chunks now'});

  // Check if there is any pending chunks that the receiver has not received yet
  final pendingChunks = share.chunks.entries.where((entry) => share.chunksSentToReceiver[entry.key] != true && share.chunksAcknowledgedByReceiver[entry.key] != true).toList();
  if (connection.isDownloadResumed && pendingChunks.isNotEmpty) { // avoid sending chunks if download has not resumed yet
    // Only send a few pending chunks to avoid overwhelming the receiver
    // Less chunks the receiver has acknowledged, more chunks we send to the receiver.
    // Maximum 3 chunks. So if ack:0, send 3. If ack:1, send 2. If ack:2, send 1. If ack:3, send 0.
    int unacknowledgedChunksAmount = pendingChunks.length;
    for (var i = 0; i < pendingChunks.length; i++) {
      if (i >= min(3, unacknowledgedChunksAmount)) break;

      final entry = pendingChunks[i];
      final chunkId = entry.key;
      final chunkData = share.chunks[chunkId];

      if (share.chunks.containsKey(i) && chunkData == null) {
        share.restartTransfer();
        return;
      }

      if (chunkData != null) {
        share.chunksSentToReceiver[chunkId] = true;
        connection.sendBinary(frameChunk(chunkId, chunkData));
      }

      // we should not remove the chunk from pendingChunks if we sent them here, because we need to wait for an ack for them
    }
  }

  if (!connection.isDownloadResumed && pendingChunks.isEmpty) {
    connection.isDownloadResumed = true;
    connection.send('downloadResumed', {'message': 'Download has been resumed, you will receive new chunks from the sender'});
  }

  // If there is no pending chunks, allow the sender to send more chunks to the receiver
  if (pendingChunks.isEmpty) share.canSendChunksToReceiver = true;
  share.touch();
  share.clearAcknowledgedChunks();
}

void _handleSengMsgToOtherWay(SocketConnection connection, Object? data) {
  ShareDetails? share = _checkSocketConnectedShare(connection);
  if(share == null) return;

  final isSender = share.senderConnection == connection;
  if (isSender) {
    share.receiverConnection?.send('msgFromSender', data);
    share.messagesFromSenderQueue.add(data);
    share.checkMessagesMemoryUsage();
    return;
  } else {
    share.senderConnection?.send('msgFromReceiver', data);
    share.messagesFromReceiverQueue.add(data);
    share.checkMessagesMemoryUsage();
    return;
  }
}

void _handleLastChunkIndication(SocketConnection connection, Object? data) {
  ShareDetails? share = _checkSocketConnectedShare(connection);
  if(share == null) return;

  final isSender = share.senderConnection == connection;
  if (!isSender) {
    connection.send('fatal', {'error': 'receiver_cannot_indicate_last_chunk', 'message': 'Receivers cannot indicate last chunk'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'receiver_cannot_indicate_last_chunk'));
    return;
  }

  if (data is! Map<String, Object?>) {
    connection.send('fatal', {'error': 'invalid_data', 'message': 'Invalid data format for LastChunk'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'invalid_data'));
    return;
  }

  int lastChunkId = 0;
  if (data['lastChunkId'] is int) {
    lastChunkId = data['lastChunkId'] as int;
  } else if (data['lastChunkId'] is String) {
    lastChunkId = int.tryParse(data['lastChunkId'] as String) ?? 0;
  }

  if (lastChunkId > share.chunks.keys.last) {
    connection.send('fatal', {'error': 'lastChunkId_too_low', 'message': 'lastChunkId cannot be lower than the last chunkId sent to the receiver'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'lastChunkId_too_low'));
    return;
  }

  share.lastChunkId = lastChunkId;
  share.touch();

  share.receiverConnection?.send('lastChunkIndicated', {'lastChunkId': lastChunkId, 'message': 'The sender has indicated the last chunk to be sent'});
}

void _handleTransferDeletion(SocketConnection connection, Object? data) {
  ShareDetails? share = _checkSocketConnectedShare(connection);
  if(share == null) return;

  final isSender = share.senderConnection == connection;
  if (!isSender) {
    connection.send('fatal', {'error': 'receiver_cannot_delete_transfer', 'message': 'Receivers cannot delete the transfer'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'receiver_cannot_delete_transfer'));
    return;
  }

  share.deleteShare();
}

ShareDetails? _checkSocketConnectedShare(SocketConnection connection) {
  String? shareId = connection.connectedShareId;
  if (!connection.isConnectedToShare || shareId == null || shareId.isEmpty) {
    connection.send('fatal', {'error': 'not_connected_to_share', 'message': 'You are not connected to any share'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'not_connected_to_share'));
    return null;
  }

  ShareDetails? share = sharedDetails[shareId];
  if (share == null) {
    connection.send('fatal', {'error': 'share_deleted', 'message': 'The share you were connected to no longer exists'});
    unawaited(connection.close(code: ws_status.normalClosure, reason: 'share_deleted'));
    return null;
  }

  return share;
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
    onDone: () {
      // Code 1000 (normal) and 1001 (going away) should not be considered as errors
      final code = channel.closeCode;
      _warnDisconnection(connection, code != null && code != 1000 && code != 1001 ? code : null);

      socketRegistry.remove(connection);
    },
    onError: (_) {
      _warnDisconnection(connection, null);
      socketRegistry.remove(connection);
    },
    cancelOnError: true,
  );

  connection.send(
    'welcome',
    {
      'connectionId': connection.id
    }
  );
}

void _warnDisconnection(SocketConnection connection, int? code) {
  // If the connection was a receiver, notify the sender that it has disconnected
  if (connection.isConnectedToShare && connection.shareRole == ShareRole.receiver) {
    sharedDetails[connection.connectedShareId]?.senderConnection?.send('receiverStatus', {'connected': false, 'message': 'Receiver has disconnected from the share'});
  }

  // If the connection was a sender, notify the receiver that it has disconnected
  if (connection.isConnectedToShare && connection.shareRole == ShareRole.sender) {
    sharedDetails[connection.connectedShareId]?.receiverConnection?.send('senderStatus', {'connected': false, 'message': 'Sender has disconnected from the share'});
  }

  // Touch the share details to update properties
  if (connection.connectedShareId is String && connection.connectedShareId!.isNotEmpty && sharedDetails.containsKey(connection.connectedShareId)) {
    sharedDetails[connection.connectedShareId]?.touch();
  }
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