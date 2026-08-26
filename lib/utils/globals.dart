const int protocolVersion = 1;
const int maxDeviceNameLength = 64;

const int maxJsonBytes = 1 << 20; // 1 MiB

int? port;
int? maxChunkBytes;
int? maxCachedBytes;
bool isProduction = false;

String? relayName;
String? relayContactEmail;
Map<String, String> relayAssociatedLinks = {};

int? availableRam;