import 'dart:convert';
import 'dart:typed_data';

String toBase64Url(List<int> bytes) => base64Url.encode(bytes).replaceAll('=', '');

Uint8List fromBase64Url(String text) {
  final padding = (4 - text.length % 4) % 4;
  return base64Url.decode(text + '=' * padding);
}