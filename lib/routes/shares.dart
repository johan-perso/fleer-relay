import 'package:fleer_relay/routes/socket.dart';
import 'package:fleer_relay/utils/base64.dart';
import 'package:fleer_relay/utils/globals.dart' as globals;
import 'package:fleer_relay/routes/_router.dart';

import 'dart:typed_data';
import 'package:nanoid2/nanoid2.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

enum ShareRole {
  sender,
  receiver,
}

class ShareDetails {
  ShareDetails({
    required this.encryptionProtocolIndicator,
    required this.filesCount,
    required this.foldersCount,
    required this.totalSize,
  }) : creation = DateTime.now().toUtc(), lastActivity = DateTime.now().toUtc();

  final int encryptionProtocolIndicator;
  final int filesCount;
  final int foldersCount;
  int totalSize;
  final DateTime creation;
  DateTime lastActivity;

  String? receiverDeviceName;
  SocketConnection? receiverConnection;
  bool canSendChunksToReceiver = false;
  List<dynamic> messagesFromReceiverQueue = [];

  SocketConnection? senderConnection;
  List<dynamic> messagesFromSenderQueue = [];

  final Map<int, List<int>?> chunks = {};
  final Map<int, bool> chunksSentToReceiver = {};
  final Map<int, bool> chunksAcknowledgedByReceiver = {};
  int receivedBytes = 0;
  int allowedBytesMax = 0;

  List<int>? primaryDetails;
  bool uploadCanStart = false;

  void touch() {
    lastActivity = DateTime.now().toUtc();
  }

  // Check if we should send chunks to the receiver based on the last 3 acknowledged chunks.
  // If there is at least 3 not acknowledged chunks, we pause sending new chunks to the receiver to avoid overwhelming it.
  void checkSendingChunksToReceiver() {
    int acknowledgedCount = 0;
    for (int i = 0; i < 3; i++) {
      final checkChunkId = chunksSentToReceiver.keys.toList().reversed.skip(i).firstWhere(
        (chunkId) => chunksSentToReceiver[chunkId] == true,
        orElse: () => -1,
      );
      if (checkChunkId >= 0 && chunksAcknowledgedByReceiver[checkChunkId] == true) {
        acknowledgedCount++;
      }
    }

    if (chunks.length <= 3) { // we have less than 3 chunks since the beginning, we can send them all
      canSendChunksToReceiver = true;
    } else if (acknowledgedCount < 3) { // means we have less than 3 acknowledged chunks
      canSendChunksToReceiver = false;
    } else { // means we have at least 3 acknowledged chunks, we can send new chunks
      canSendChunksToReceiver = true;
    }
  }

  // Free up some memory by clearing acknowledged chunks
  void clearAcknowledgedChunks() {
    final acknowledgedChunks = chunksAcknowledgedByReceiver.entries.where((entry) => entry.value == true).toList();
    for (final entry in acknowledgedChunks) {
      final chunkId = entry.key;
      if(chunks.containsKey(chunkId)) chunks[chunkId] = null; // only clear the data, keep the key to avoid skipping chunks
    }
  }

  // Method to restart the transfer if needed (ex: the receiver requested a chunk that was already sent but cleared from memory)
  void restartTransfer({
    String? textualReason = 'The entire transfer will be restarted because the sender need to send again a few data that were already sent to the receiver'
  }) {
    receivedBytes = 0;
    allowedBytesMax = globals.maxCachedBytes!;

    uploadCanStart = true;
    canSendChunksToReceiver = false;

    messagesFromReceiverQueue.clear();
    messagesFromSenderQueue.clear();

    chunks.clear();
    chunksSentToReceiver.clear();
    chunksAcknowledgedByReceiver.clear();

    receiverConnection?.send('restartTransfer', {'message': textualReason});
    senderConnection?.send('restartTransfer', {'message': textualReason});
    receiverConnection?.isDownloadResumed = true;

    touch();
  }
}

final sharedDetails = <String, ShareDetails>{};

Router sharesRoutes() {
  return Router()
    ..post('/shares/read', (Request request) => _readShare(request))
    ..post('/shares/create', (Request request) => _createShare(request))
    ..put('/shares/chunks', (Request request) => _putChunk(request));
}

Future<Response> _createShare(Request request) async {
  final body = await readJsonBody(request, maxBytes: globals.maxJsonBytes);
  if (body is! Map<String, Object?>) {
    throw HttpError(400, 'body_invalid_content', 'Your request is missing a valid JSON body');
  }

  final encryptionProtocolIndicator = _readCount(body, 'encryptionProtocolIndicator');
  if (encryptionProtocolIndicator < 1) {
    throw HttpError(400, 'invalid_encryptionProtocolIndicator', 'encryptionProtocolIndicator must be at least 1');
  }

  final filesCount = _readCount(body, 'filesCount');
  final foldersCount = _readCount(body, 'foldersCount');

  final totalSize = _readCount(body, 'totalSize');
  if(totalSize < 1) {
    throw HttpError(400, 'invalid_totalSize', 'totalSize must be at least 1 byte');
  }

  final shareId = _generateShareId();

  sharedDetails[shareId] = ShareDetails(
    encryptionProtocolIndicator: encryptionProtocolIndicator,
    filesCount: filesCount,
    foldersCount: foldersCount,
    totalSize: totalSize,
  );

  return jsonOk({'shareId': shareId});
}

Future<Response> _readShare(Request request) async {
  final body = await readJsonBody(request, maxBytes: globals.maxJsonBytes);
  if (body is! Map<String, Object?>) {
    throw HttpError(400, 'body_invalid_content', 'Your request is missing a valid JSON body');
  }

  String shareId = body['shareId']?.toString() ?? '';
  if (shareId.isEmpty) {
    throw HttpError(400, 'missing_shareId', 'Missing shareId field in body');
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    throw HttpError(404, 'share_not_found', 'No share with the provided shareId was found');
  }

  return jsonOk({
    'shareId': shareId,
    'filesCount': share.filesCount,
    'foldersCount': share.foldersCount,
    'receivedBytes': share.receivedBytes,
    'totalSize': share.totalSize,
    'creation': share.creation.toIso8601String(),
    'encryptionProtocolIndicator': share.encryptionProtocolIndicator,
    'primaryDetails': share.primaryDetails == null
      ? null
      : toBase64Url(share.primaryDetails!),
  });
}

Future<Response> _putChunk(Request request) async {
  final shareId = request.url.queryParameters['shareId'];
  if (shareId == null || shareId.isEmpty) {
    throw HttpError(400, 'missing_shareId', 'Missing shareId query parameter');
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    throw HttpError(404, 'share_not_found', 'No share with the provided shareId was found');
  }

  final isThisPrimaryDetails = request.url.queryParameters['isThisPrimaryDetails'] == 'true';
  if(isThisPrimaryDetails && (share.receivedBytes > 0 || share.primaryDetails != null || share.uploadCanStart)) {
    throw HttpError(400, 'primary_details_already_received', 'Primary details have already been received for this share');
  }

  final chunkIdStr = request.url.queryParameters['chunkId'];
  if (!isThisPrimaryDetails && (chunkIdStr == null || chunkIdStr.isEmpty)) {
    throw HttpError(400, 'missing_chunkId', 'Missing chunkId query parameter');
  }

  int? chunkId;
  if (chunkIdStr is! num) {
     chunkId = int.tryParse(chunkIdStr ?? '');
  }
  if (chunkId != null && (chunkId < 0 || chunkId > 4294967295 - 1)) { // highest value for a 32-bit unsigned integer is 4294967295, but we subtract 1 to avoid potential overflow issues
    throw HttpError(400, 'invalid_chunkId', 'chunkId must be a non-negative integer between 0 and 4294967294');
  }

  final contentType = request.headers['content-type'] ?? '';
  if (!contentType.startsWith('application/octet-stream')) {
    throw HttpError(415, 'unsupported_body_type', 'Expected an application/octet-stream body');
  }

  if (share.primaryDetails == null && !isThisPrimaryDetails) {
    throw HttpError(400, 'primary_details_not_received', 'Primary details must be sent before any other chunks');
  }

  if (!isThisPrimaryDetails && !share.uploadCanStart) {
    throw HttpError(400, 'upload_not_started', 'Upload cannot start until primary details have been received');
  }

  if (!isThisPrimaryDetails && share.receivedBytes >= share.allowedBytesMax) {
    throw HttpError(400, 'wait_before_uploading', 'Upload limit reached for this share. Wait for the receiver to download some data before uploading more.');
  }

  final data = await readBinaryBody(request, maxBytes: globals.maxChunkBytes!);
  if (data.isEmpty) throw HttpError(400, 'missing_body', 'Chunk body is empty');

  if (!isThisPrimaryDetails && share.receivedBytes + data.length > share.allowedBytesMax) {
    throw HttpError(400, 'wait_before_uploading', 'Upload limit reached for this share. Wait for the receiver to download some data before uploading more.');
  }

  if (isThisPrimaryDetails) {
    share.primaryDetails = data;
    share.allowedBytesMax = globals.maxCachedBytes!;
    share.uploadCanStart = true;
    share.touch();
  } else {
    if (chunkId == null) {
      throw HttpError(400, 'missing_chunkId', 'Missing chunkId query parameter, or it is not a valid integer');
    }

    // Avoid skipping chunks by checking if the chunk before this one has been received. If not, we return an error.
    final previousChunkId = chunkId - 1;
    if (previousChunkId >= 0 && !share.chunks.containsKey(previousChunkId)) {
      throw HttpError(400, 'missing_previous_chunk', 'The previous chunk (chunkId: $previousChunkId) has not been received yet. Please send chunks in order.');
    }

    share.chunks[chunkId] = data;
    share.receivedBytes += data.length;
    share.touch();

    if (share.totalSize < share.receivedBytes) { // clients can send more than the size they declared
      share.totalSize = share.receivedBytes;
    }

    // Check if the receiver should receive this chunk
    share.clearAcknowledgedChunks();
    share.checkSendingChunksToReceiver();

    if (share.canSendChunksToReceiver) {
      share.chunksSentToReceiver[chunkId] = true;
      share.receiverConnection?.sendBinary(frameChunk(chunkId, data));
    }
  }

  return jsonOk({
    'chunkId': chunkId,
    'bytes': data.length,
    'receivedBytes': share.receivedBytes,
    'allowedBytesMax': share.allowedBytesMax,
  });
}

Uint8List frameChunk(int index, List<int> payload) {
  final out = Uint8List(5 + payload.length);
  out[0] = 0; // 0 = file chunk
  ByteData.view(out.buffer).setUint32(1, index, Endian.big);
  out.setRange(5, out.length, payload);
  return out;
}

int _readCount(Map<String, Object?> body, String field) {
  final raw = body[field] ?? 0;

  if (raw is! num || raw < 0 || raw.sign == -1) {
    throw HttpError(400, 'invalid_number', '$field must be a non-negative number');
  }
  return raw.toInt();
}

String _generateShareId() {
  const baseLength = 9;
  var attempt = 0;

  while (true) {
    final id = nanoid(length: baseLength + attempt ~/ 5, alphabet: Alphabet.noDoppelganger);
    if (!sharedDetails.containsKey(id)) return id;
    attempt++;
  }
}