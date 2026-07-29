const int protocolVersion = 1;
const int maxDeviceNameLength = 64;

const int maxJsonBytes = 1 << 20; // 1 Mio

int? port;
int? maxChunkBytes;
bool isProduction = false;