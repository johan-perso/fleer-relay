import 'package:fleer_relay/routes/_router.dart';
import 'package:fleer_relay/routes/shares.dart';
import 'package:fleer_relay/routes/socket.dart';
import 'package:fleer_relay/utils/getAvailableRam.dart';
import 'package:fleer_relay/utils/load_env.dart';
import 'package:fleer_relay/utils/globals.dart' as globals;

import 'dart:async';
import 'dart:io';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

DateTime? lastAvailableRamCheck;

Future<void> main() async {
  globals.port = getValueFromEnv(key: 'PORT', fallback: 8080, type: int);
  globals.maxChunkBytes = getValueFromEnv(key: 'MAX_CHUNK_BYTES', fallback: (10 << 20), type: int); // 10 MiB
  globals.maxCachedBytes = getValueFromEnv(key: 'MAX_CACHED_BYTES', fallback: (100 << 20), type: int); // 100 MiB
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
  shelf_io.serveRequests(server, handler, poweredByHeader: 'Fleer Relay Server');

  stdout.writeln('Fleer Relay Server listening on http://0.0.0.0:${globals.port}');
  stdout.writeln(repeat('-', 40));
  stdout.writeln('Max JSON body size:      ${globals.maxJsonBytes} bytes');
  stdout.writeln('Max chunked file size:   ${globals.maxChunkBytes} bytes');
  stdout.writeln('Environment:             ${globals.isProduction ? 'production' : 'development'}');
  stdout.writeln(repeat('-', 40));

  // Simple check every minute to avoid taking too much memory
  Timer.periodic(const Duration(minutes: 1), (_) {
    // Clear inactive WebSocket connections to free up memory
    socketRegistry.cleanInactiveConnections();

    // Clear shares that are inactive
    for(final share in sharedDetails.values.toList()) {
      DateTime lastActivity = share.lastActivity;
      bool isAnyoneConnected = share.receiverConnection != null || share.senderConnection != null;

      // Delete the share if nobody is connected and the share has been inactive for more than 5 minutes
      if(!isAnyoneConnected && DateTime.now().difference(lastActivity) > const Duration(minutes: 5)) {
        share.deleteShare(textualReason: 'The share has been deleted because nobody was connected and it has been inactive for more than 5 minutes.');
        continue;
      }

      // Delete the share if it has been inactive for more than 10 minutes, even if someone is connected
      if(DateTime.now().difference(lastActivity) > const Duration(minutes: 10)) {
        share.deleteShare(textualReason: 'The share has been deleted because it has been inactive for more than 10 minutes.');
        continue;
      }
    }
  });

  // Check available amount of RAM every 30 seconds (it need to be checked often but not too often, even if it's not really expensive to run)
  Timer.periodic(const Duration(seconds: 5), (_) async {
    if (lastAvailableRamCheck != null && DateTime.now().difference(lastAvailableRamCheck!) < const Duration(seconds: 30)) {
      return; // Avoid checking too often
    }

    globals.availableRam = await readMemoryInfo().available;
    lastAvailableRamCheck = DateTime.now();
  });
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

String repeat(String str, int times) {
  return List.filled(times, str).join();
}