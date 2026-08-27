import 'package:fleer_relay/routes/shares.dart';
import 'package:fleer_relay/routes/socket.dart';
import 'package:fleer_relay/utils/globals.dart' as globals;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

const _headers = {'content-type': 'application/json; charset=utf-8'};

final String? buildVersion = _readBuildFile('CURRENT_VERSION.txt');
final String? buildCommitHash = _readBuildFile('LATEST_COMMIT_HASH.txt');

String? _readBuildFile(String fileName) {
  final sep = Platform.pathSeparator;

  // resolvedExecutable, if the server is compiled to a standalone executable, will point to the executable itself
  final exeDir = File(Platform.resolvedExecutable).parent;

  final candidates = <File>[
    File('${exeDir.parent.path}$sep$fileName'), // when building with Docker, file is copied to /app but server is in /app/bin so we go backwards
    File(fileName), // also the current working directory, in case the file might be here
  ];

  for (final file in candidates) {
    try {
      if (!file.existsSync()) continue;
      final value = file.readAsStringSync().trim();
      if (value.isEmpty || value == 'unknown') return null;
      return value;
    } on FileSystemException {
      continue; // missing permissions for example
    }
  }
  return null;
}

Router buildRouter() {
  return Router(
    notFoundHandler: (_) => jsonError(
      404,
      'not_found',
      message: 'The requested endpoint does not exist'
    )
  )
    ..get('/', (Request _) { // health/status endpoint
      if (globals.availableRam == null) {
        throw HttpError(503, 'server_not_ready', 'The server is not ready yet. Please try again in a few seconds.');
      }
      if (!isRamSufficientForNewShare()) {
        throw HttpError(503, 'server_too_busy', 'The server is processing too many transfers at the moment. Please try again in a few seconds.');
      }

      return jsonOk({
        'message': 'Fleer Relay API is running',
        'sharesCreationAllowed': isRamSufficientForNewShare(),
        'server': {
          'buildVersion': buildVersion,
          'buildCommitHash': buildCommitHash,
          'protocolVersion': globals.protocolVersion,
          'maxDeviceNameLength': globals.maxDeviceNameLength,
          'maxJsonBytes': globals.maxJsonBytes,
          'maxChunkBytes': globals.maxChunkBytes,
          'maxCachedBytes': globals.maxCachedBytes,
        },
        'relay': {
          'name': globals.relayName,
          'contactEmail': globals.relayContactEmail,
          'associatedLinks': Map<String, String>.from(globals.relayAssociatedLinks)..removeWhere((key, value) => value.isEmpty),
        },
      });
    })
    ..post('/shares/read', sharesRoutes().call)
    ..get('/shares/updates', socketHandler())
    ..post('/shares/create', sharesRoutes().call)
    ..put('/shares/chunks', sharesRoutes().call);
}

Response jsonOk(Object? data) => Response.ok(jsonEncode({'data': data}), headers: _headers);
Response jsonError(int status, String code, {String? message}) => Response(
  status,
  body: jsonEncode({'error': code, if (message != null) 'message': message}),
  headers: _headers,
);

class HttpError implements Exception {
  HttpError(this.status, this.code, [this.message]);
  final int status;
  final String code;
  final String? message;
}

Future<Object?> readJsonBody(Request request, {required int maxBytes}) async {
  final declared = request.contentLength;
  if (declared != null && declared > maxBytes) {
    throw HttpError(413, 'body_too_large', 'Body exceeds the maximum size of $maxBytes bytes');
  }

  final builder = BytesBuilder(copy: false);

  await for (final chunk in request.read()) {
    builder.add(chunk);

    if (builder.length > maxBytes) {
      throw HttpError(413, 'body_too_large', 'Body exceeds the maximum size of $maxBytes bytes');
    }
  }

  final bytes = builder.takeBytes();
  if (bytes.isEmpty) {
    throw HttpError(400, 'body_invalid_content', 'Your request is missing a valid JSON body');
  }

  final String text;
  try {
    text = utf8.decode(bytes);
  } on FormatException {
    throw HttpError(400, 'body_invalid_utf8', 'Body is not valid UTF-8');
  }

  try {
    return jsonDecode(text);
  } on FormatException {
    throw HttpError(400, 'body_invalid_json', 'Your request is missing a valid JSON body');
  }
}

Future<Uint8List> readBinaryBody(Request request, {required int maxBytes}) async {
  maxBytes += (maxBytes * 0.0001).toInt(); // Add 0.01% to account for overhead (due to encryption)

  final declared = request.contentLength;
  if (declared != null && declared > maxBytes) {
    throw HttpError(413, 'body_too_large', 'Body exceeds the maximum size of $maxBytes bytes');
  }

  final builder = BytesBuilder(copy: false);
  await for (final chunk in request.read()) {
    builder.add(chunk);
    if (builder.length > maxBytes) {
      throw HttpError(413, 'body_too_large', 'Body exceeds the maximum size of $maxBytes bytes');
    }
  }
  return builder.takeBytes();
}

bool isRamSufficientForNewShare() {
  if (globals.availableRam == null) {
    return false;
  }
  if (globals.availableRam! < globals.maxCachedBytes! * 2) { // a share can use up to maxCachedBytes, adding a safety margin of one additional share
    return false;
  }
  if (globals.availableRam! < 100 * 1024 * 1024) { // at least 100 MiB of RAM should be available on the host
    return false;
  }
  return true;
}

bool isRamSufficientForChunkSave() {
  if (globals.availableRam == null) {
    return false;
  }
  if (globals.availableRam! < globals.maxCachedBytes!) {
    return false;
  }
  if (globals.availableRam! < 100 * 1024 * 1024) { // at least 100 MiB of RAM should be available on the host
    return false;
  }
  return true;
}