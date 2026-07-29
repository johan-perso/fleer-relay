import 'package:fleer_backend/utils/globals.dart' as globals;
import 'package:fleer_backend/routes/_router.dart';

import 'package:nanoid2/nanoid2.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

class ShareDetails {
  ShareDetails({
    required this.filesCount,
    required this.foldersCount,
    required this.totalSize,
  }) : creation = DateTime.now().toUtc(), lastActivity = DateTime.now().toUtc();

  final int filesCount;
  final int foldersCount;
  final int totalSize;
  final DateTime creation;
  DateTime lastActivity;

  final Map<String, List<int>> chunks = {};
  List<int>? primaryDetails;
  bool downloadCanStart = false;
  bool downloadStarted = false;
  int receivedBytes = 0;

  Map<String, Object?> toJson() => {
    'filesCount': filesCount,
    'foldersCount': foldersCount,
    'receivedBytes': receivedBytes,
    'totalSize': totalSize,
    'creation': creation.toIso8601String(),
  };

  void touch() {
    lastActivity = DateTime.now().toUtc();
  }
}

final sharedDetails = <String, ShareDetails>{};

Router sharesRoutes() {
  return Router()
    ..get('/shares/read', (Request request) => _readShare(request))
    ..post('/shares/create', (Request request) => _createShare(request))
    ..put('/shares/chunks', (Request request) => _putChunk(request));
}

Future<Response> _createShare(Request request) async {
  final body = await readJsonBody(request, maxBytes: globals.maxJsonBytes);
  if (body is! Map<String, Object?>) {
    throw HttpError(400, 'body_invalid_content', 'Your request is missing a valid JSON body');
  }

  final filesCount = _readCount(body, 'filesCount');
  final foldersCount = _readCount(body, 'foldersCount');

  final totalSize = _readCount(body, 'totalSize');
  if(totalSize < 1) {
    throw HttpError(400, 'invalid_totalSize', 'totalSize must be at least 1 byte');
  }

  final shareId = _generateShareId();

  sharedDetails[shareId] = ShareDetails(
    filesCount: filesCount,
    foldersCount: foldersCount,
    totalSize: totalSize,
  );

  return jsonOk({'shareId': shareId});
}

Future<Response> _readShare(Request request) async {
  final shareId = request.url.queryParameters['shareId'];
  if (shareId == null || shareId.isEmpty) {
    throw HttpError(400, 'missing_shareId', 'Missing shareId query parameter');
  }

  final share = sharedDetails[shareId];
  if (share == null) {
    throw HttpError(404, 'share_not_found', 'No share with the provided shareId was found');
  }

  return jsonOk(share.toJson());
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

  final chunkId = request.url.queryParameters['chunkId'];
  if (chunkId == null || chunkId.isEmpty) {
    throw HttpError(400, 'missing_chunkId', 'Missing chunkId query parameter');
  }

  final isThisPrimaryDetails = request.url.queryParameters['isThisPrimaryDetails'] == 'true';
  if(isThisPrimaryDetails && (share.receivedBytes > 0 || share.primaryDetails != null || share.downloadCanStart)) {
    throw HttpError(400, 'primary_details_already_received', 'Primary details have already been received for this share');
  }

  final contentType = request.headers['content-type'] ?? '';
  if (!contentType.startsWith('application/octet-stream')) {
    throw HttpError(415, 'unsupported_body_type', 'Expected an application/octet-stream body');
  }

  if (share.primaryDetails == null && !isThisPrimaryDetails) {
    throw HttpError(400, 'primary_details_not_received', 'Primary details must be sent before any other chunks');
  }

  final data = await readBinaryBody(request, maxBytes: globals.maxChunkBytes!);
  if (data.isEmpty) throw HttpError(400, 'missing_body', 'Chunk body is empty');

  if(isThisPrimaryDetails) {
    share.primaryDetails = data;
    share.downloadCanStart = true;
  } else {
    share.chunks[chunkId] = data;
    share.receivedBytes += data.length;
  }

  return jsonOk({
    'chunkId': chunkId,
    'bytes': data.length,
    'receivedBytes': share.receivedBytes,
    'totalSize': share.totalSize,
  });
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