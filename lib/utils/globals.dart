const int protocolVersion = 1;
const int maxDeviceNameLength = 64;

const int maxJsonBytes = 1 << 20; // 1 MiB

int? port;
int? maxChunkBytes;
int? maxCachedBytes;
bool isProduction = false;

int? availableRam;