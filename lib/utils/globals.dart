const int protocolVersion = 1;

const int maxJsonBytes = 1 << 20; // 1 MiB
const int maxSocketFrameBytes = 64 << 10; // 64 KiB

int? port;
int? maxChunkBytes;
int? maxCachedBytes;
bool isProduction = false;

String? relayName;
String? relayContactEmail;
Map<String, String> relayAssociatedLinks = {};

int? availableRam;