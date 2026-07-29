import 'package:fleer_backend/routes/_router.dart';
import 'package:fleer_backend/utils/load_env.dart';
import 'package:fleer_backend/utils/globals.dart' as globals;
import 'package:fleer_backend/utils/repeat.dart';

import 'dart:async';
import 'dart:io';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

Future<void> main() async {
  globals.port = getValueFromEnv(key: 'PORT', fallback: 8080, type: int);
  globals.maxChunkBytes = getValueFromEnv(key: 'MAX_CHUNK_BYTES', fallback: (10 << 20), type: int); // 10 Mio
  globals.isProduction = getValueFromEnv(key: 'ENV', fallback: 'dev', type: String) == 'production';

  await _serve();
}

Future<void> _serve() async {
  final server = await HttpServer.bind(
    InternetAddress.anyIPv4,
    globals.port!,
    shared: true,
  );
  server.autoCompress = true;
  server.idleTimeout = const Duration(seconds: 75);

  Pipeline pipeline = const Pipeline();
  if (!globals.isProduction) pipeline = pipeline.addMiddleware(logRequests());
  pipeline = pipeline.addMiddleware(_catchErrors());

  final Handler handler = pipeline.addHandler(buildRouter().call);
  shelf_io.serveRequests(server, handler);

  stdout.writeln('Server listening on http://0.0.0.0:${globals.port}');
  stdout.writeln(repeat('-', 40));
  stdout.writeln('Max JSON body size:      ${globals.maxJsonBytes} bytes');
  stdout.writeln('Max chunked file size:   ${globals.maxChunkBytes} bytes');
  stdout.writeln('Environment:             ${globals.isProduction ? 'production' : 'development'}');
  stdout.writeln(repeat('-', 40));
}

Middleware _catchErrors() {
  return (Handler inner) => (Request request) async {
    try {
      return await inner(request);
    } on HijackException {
      rethrow; // not an error (WebSocket Upgrade)
    } on HttpError catch (e) {
      return jsonError(e.status, e.code, message: e.message);
    } catch (error, stack) {
      stderr.writeln('${request.method} ${request.requestedUri}\n$error\n$stack');
      return jsonError(500, 'internal_error');
    }
  };
}