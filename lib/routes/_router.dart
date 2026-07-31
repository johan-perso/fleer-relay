import 'dart:typed_data';

import 'package:fleer_backend/routes/shares.dart';
import 'package:fleer_backend/routes/socket.dart';
import 'package:fleer_backend/utils/globals.dart' as globals;

import 'dart:convert';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

const _headers = {'content-type': 'application/json; charset=utf-8'};

Router buildRouter() {
  return Router(
    notFoundHandler: (_) => jsonError(
      404,
      'not_found',
      message: 'The requested endpoint does not exist'
    )
  )
    ..get('/', (Request _) {
      return jsonOk({ 'message': 'Fleer Backend API is running', 'server': {
        'protocolVersion': globals.protocolVersion,
        'maxDeviceNameLength': globals.maxDeviceNameLength,
        'maxJsonBytes': globals.maxJsonBytes,
      } });
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