// Libs such as system_info2 and system_info3 on pub.dev are no longer maintained and have major bugs on Apple Silicon chips and the latest Windows versions.
// I didn't want to bother making something huge and complex myself, so I simply asked Claude, here is it 💀

/// Physical memory reporting without any dependency beyond `package:ffi`.
/// Supported platforms: Linux, Android, Windows, macOS.
///
/// No subprocesses are spawned (`vm_stat`, `wmic`, `free`, ...): every value
/// comes from a file read or a direct native call.
library;

import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:math' as math;

import 'package:ffi/ffi.dart';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// What bounds [MemoryInfo.total].
enum MemoryLimitSource {
  /// The physical machine, as reported by the kernel.
  host,

  /// A cgroup memory limit (container, systemd slice, ...) tighter than the
  /// host's physical memory.
  cgroup,
}

class MemoryInfo {
  const MemoryInfo({
    required this.total,
    required this.available,
    required this.totalLimitedBy,
  });

  /// Total memory available to the process from the operating system's point
  /// of view, in bytes.
  ///
  /// Under a cgroup limit this is the limit rather than the machine's physical
  /// RAM, since the latter has no bearing on what the OOM killer will
  /// tolerate. It is always the smaller of the two, so a cgroup limit larger
  /// than physical memory does not inflate this value.
  ///
  /// Not every platform limit is reflected here: on Windows this is physical
  /// memory, and a Job Object memory limit — the mechanism Windows containers
  /// use — is not taken into account.
  final int total;

  /// Estimated memory that can be allocated without swapping, in bytes.
  ///
  /// This is an estimate, not a guarantee. Linux's own `MemAvailable` is
  /// documented as an estimate, and the cgroup figure is derived from a
  /// working-set heuristic. Reclaimable page cache counts as available even
  /// though it is not free. Treat this as a budgeting signal, not as a
  /// precondition for a specific allocation.
  ///
  /// May be bounded by a cgroup even when [totalLimitedBy] is
  /// [MemoryLimitSource.host]: a cgroup whose limit equals physical memory
  /// still constrains how much of it is left.
  final int available;

  /// Whether [total] is bounded by the host or by a cgroup.
  ///
  /// This describes [total] only — see [available] for why the two can
  /// disagree.
  final MemoryLimitSource totalLimitedBy;

  int get used => total - available;

  /// Available fraction, between 0.0 and 1.0.
  double get availableRatio => total == 0 ? 0 : available / total;

  @override
  String toString() {
    String mib(int bytes) =>
        '${(bytes / (1024 * 1024)).toStringAsFixed(0)} MiB';
    return 'MemoryInfo(total: ${mib(total)}, available: ${mib(available)}, '
        'totalLimitedBy: ${totalLimitedBy.name})';
  }
}

/// Reads the current memory state.
///
/// Throws [UnsupportedError] on unsupported platforms (iOS, web), and
/// [StateError] if the platform reports something unusable.
MemoryInfo readMemoryInfo() {
  if (Platform.isLinux || Platform.isAndroid) return linuxMemoryInfo();
  if (Platform.isWindows) return _windowsMemoryInfo();
  if (Platform.isMacOS) return _macosMemoryInfo();
  throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
}

/// Sanity-checks a reading. Call once at startup: failing loudly beats sizing
/// caches against a bogus number.
///
/// Guards against the class of bug where a library multiplies an
/// already-in-bytes value by the page size, inflating memory 4096x or 16384x.
void validateMemoryReading() {
  final info = readMemoryInfo();

  const minPlausible = 64 * 1024 * 1024; // 64 MiB
  const maxPlausible = 8 * 1024 * 1024 * 1024 * 1024; // 8 TiB

  if (info.total < minPlausible || info.total > maxPlausible) {
    throw StateError('Implausible total memory: ${info.total} bytes');
  }
  if (info.available < 0 || info.available > info.total) {
    throw StateError(
        'Inconsistent available memory: ${info.available} of ${info.total}');
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Reads a file, returning null on any failure.
///
/// Pseudo-filesystem reads fail in ways `existsSync()` cannot predict: EACCES
/// under a hardened container or on Android, EOPNOTSUPP on some controllers,
/// or the file disappearing between the check and the read.
String? _tryReadFile(String path) {
  try {
    return File(path).readAsStringSync();
  } on FileSystemException {
    return null;
  }
}

int? _tryReadInt(String path) {
  final raw = _tryReadFile(path)?.trim();
  return raw == null ? null : int.tryParse(raw);
}

// ---------------------------------------------------------------------------
// Linux / Android
// ---------------------------------------------------------------------------

/// Linux/Android implementation.
///
/// The paths are parameters so the cgroup and meminfo logic can be exercised
/// against fixture directories in tests. Production code calls
/// [readMemoryInfo] and leaves them at their defaults.
MemoryInfo linuxMemoryInfo({
  String meminfoPath = '/proc/meminfo',
  String procSelfCgroupPath = '/proc/self/cgroup',
  String cgroupRoot = '/sys/fs/cgroup',
}) {
  final host = _hostMemInfo(meminfoPath);
  final cgroup = _cgroupConstraint(
    procSelfCgroupPath: procSelfCgroupPath,
    cgroupRoot: cgroupRoot,
  );

  if (cgroup == null) return host;

  // Both constraints apply at once, so the effective figures are the tighter
  // of the two: a cgroup limit above physical RAM must not inflate the total,
  // and host memory pressure caps availability no matter how much headroom
  // the cgroup nominally allows.
  final total = math.min(host.total, cgroup.limit);

  // A null headroom means the limit is known but current usage is not. The
  // limit still applies, so it caps the total; availability falls back to the
  // host estimate. Inventing a usage of zero here would silently report the
  // full limit as free.
  final headroom = cgroup.headroom;
  final available = headroom == null
      ? math.min(host.available, total)
      : math.max(0, math.min(math.min(host.available, headroom), total));

  return MemoryInfo(
    total: total,
    available: available,
    totalLimitedBy: cgroup.limit < host.total
        ? MemoryLimitSource.cgroup
        : MemoryLimitSource.host,
  );
}

/// Matches `MemTotal:       16316456 kB`.
///
/// Requiring the `kB` suffix skips the `HugePages_*` entries, which are page
/// counts rather than sizes.
final RegExp _memInfoEntry = RegExp(r'^(\w+):\s+(\d+)\s+kB$', multiLine: true);

MemoryInfo _hostMemInfo(String meminfoPath) {
  final raw = _tryReadFile(meminfoPath);
  if (raw == null) throw StateError('Cannot read $meminfoPath');

  final values = <String, int>{};
  for (final match in _memInfoEntry.allMatches(raw)) {
    values[match.group(1)!] = int.parse(match.group(2)!) * 1024;
  }

  final total = values['MemTotal'];
  if (total == null) throw StateError('$meminfoPath: MemTotal not found');

  // MemAvailable has existed since Linux 3.14 and accounts for reclaimable
  // cache. Approximate it on older kernels.
  final available = values['MemAvailable'] ??
      ((values['MemFree'] ?? 0) +
          (values['Buffers'] ?? 0) +
          (values['Cached'] ?? 0));

  return MemoryInfo(
    total: total,
    available: math.min(available, total),
    totalLimitedBy: MemoryLimitSource.host,
  );
}

class _CgroupConstraint {
  const _CgroupConstraint({required this.limit, required this.headroom});

  /// The memory limit in bytes. Always known when this object exists.
  final int limit;

  /// Estimated bytes left under [limit], or null when current usage could not
  /// be read.
  final int? headroom;
}

/// Resolves the memory limit that actually applies to this process, or null
/// when no limit is in effect.
///
/// The v1/v2 choice is made per controller rather than per hierarchy. A system
/// can run the unified hierarchy while leaving some controllers on v1, so the
/// presence of `cgroup.controllers` does not imply that *memory* is on v2.
/// `/proc/self/cgroup` states it directly: a `N:memory:/path` line means v1,
/// a `0::/path` line means the controller is on the unified hierarchy.
_CgroupConstraint? _cgroupConstraint({
  required String procSelfCgroupPath,
  required String cgroupRoot,
}) {
  final procSelf = _tryReadFile(procSelfCgroupPath);
  if (procSelf == null) return null;

  final lines = const LineSplitter().convert(procSelf);

  // v1 first: if the memory controller has its own hierarchy entry, that is
  // where its limit lives, whatever else is mounted.
  final v1Path = _memoryV1Path(lines);
  if (v1Path != null) {
    final v1 = _tightestConstraint(
      base: '$cgroupRoot/memory',
      relative: v1Path,
      readLevel: _readV1Level,
    );
    if (v1 != null) return v1;
  }

  final v2Path = _unifiedPath(lines);
  if (v2Path != null) {
    return _tightestConstraint(
      base: cgroupRoot,
      relative: v2Path,
      readLevel: _readV2Level,
    );
  }

  return null;
}

/// Extracts the path from a legacy entry such as `9:cpu,memory:/docker/abc`.
///
/// Named-only entries like `1:name=systemd:/...` are not controllers and are
/// correctly skipped by the exact-match test.
String? _memoryV1Path(List<String> lines) {
  for (final line in lines) {
    final parts = line.split(':');
    if (parts.length >= 3 && parts[1].split(',').contains('memory')) {
      // Rejoin in case the path itself contains a colon.
      return parts.sublist(2).join(':');
    }
  }
  return null;
}

/// Extracts the path from the unified entry `0::/relative/path`.
String? _unifiedPath(List<String> lines) {
  for (final line in lines) {
    if (line.startsWith('0::')) return line.substring(3);
  }
  return null;
}

_CgroupConstraint? _readV2Level(String dir) {
  final raw = _tryReadFile('$dir/memory.max')?.trim();
  if (raw == null || raw == 'max') return null;

  final limit = int.tryParse(raw);
  if (limit == null) return null;

  final current = _tryReadInt('$dir/memory.current');
  if (current == null) {
    return _CgroupConstraint(limit: limit, headroom: null);
  }

  // memory.current includes page cache, which is reclaimable under pressure.
  // Subtracting inactive_file approximates the working set; without it,
  // headroom is badly underestimated for a process that has read many files.
  //
  // Defaulting inactive_file to 0 when unreadable is safe in a way that
  // defaulting current to 0 is not: it can only shrink the reported headroom.
  final inactiveFile = _cgroupStatField('$dir/memory.stat', 'inactive_file');
  final workingSet = math.max(0, current - inactiveFile);

  return _CgroupConstraint(
    limit: limit,
    headroom: math.max(0, limit - workingSet),
  );
}

_CgroupConstraint? _readV1Level(String dir) {
  final limit = _tryReadInt('$dir/memory.limit_in_bytes');
  // With no limit set, the kernel stores a sentinel close to 2^63, rounded
  // down to a page boundary.
  if (limit == null || limit > (1 << 62)) return null;

  final usage = _tryReadInt('$dir/memory.usage_in_bytes');
  if (usage == null) {
    return _CgroupConstraint(limit: limit, headroom: null);
  }

  final inactiveFile =
      _cgroupStatField('$dir/memory.stat', 'total_inactive_file');
  final workingSet = math.max(0, usage - inactiveFile);

  return _CgroupConstraint(
    limit: limit,
    headroom: math.max(0, limit - workingSet),
  );
}

/// Walks from the process's cgroup up to the hierarchy root, keeping the
/// tightest limit and the tightest headroom found.
///
/// The two are minimised independently: each ancestor constrains the process
/// on its own, so the binding limit and the binding headroom need not come
/// from the same level. Headroom stays null if no level reported one.
_CgroupConstraint? _tightestConstraint({
  required String base,
  required String relative,
  required _CgroupConstraint? Function(String dir) readLevel,
}) {
  int? limit;
  int? headroom;

  for (final dir in cgroupPathChain(base, relative)) {
    final level = readLevel(dir);
    if (level == null) continue;

    limit = limit == null ? level.limit : math.min(limit, level.limit);

    final levelHeadroom = level.headroom;
    if (levelHeadroom != null) {
      headroom =
          headroom == null ? levelHeadroom : math.min(headroom, levelHeadroom);
    }
  }

  if (limit == null) return null;
  return _CgroupConstraint(limit: limit, headroom: headroom);
}

/// Directories from the process's cgroup up to [base], innermost first.
///
/// Under a cgroup namespace (the Docker default) [relative] is just "/", so
/// this yields [base] alone. Without a namespace it yields the full chain.
/// Levels that do not exist locally are skipped rather than aborting the walk,
/// since a namespace may hide intermediate directories.
List<String> cgroupPathChain(String base, String relative) {
  final segments = relative.split('/').where((s) => s.isNotEmpty).toList();
  final chain = <String>[];

  for (var depth = segments.length; depth >= 0; depth--) {
    final dir = depth == 0 ? base : '$base/${segments.take(depth).join('/')}';
    if (Directory(dir).existsSync()) chain.add(dir);
  }

  return chain;
}

int _cgroupStatField(String path, String field) {
  final raw = _tryReadFile(path);
  if (raw == null) return 0;

  for (final line in const LineSplitter().convert(raw)) {
    final space = line.indexOf(' ');
    if (space > 0 && line.substring(0, space) == field) {
      return int.tryParse(line.substring(space + 1).trim()) ?? 0;
    }
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Windows — GlobalMemoryStatusEx (kernel32.dll)
// ---------------------------------------------------------------------------

final class _MemoryStatusEx extends Struct {
  @Uint32()
  external int dwLength;
  @Uint32()
  external int dwMemoryLoad;
  @Uint64()
  external int ullTotalPhys;
  @Uint64()
  external int ullAvailPhys;
  @Uint64()
  external int ullTotalPageFile;
  @Uint64()
  external int ullAvailPageFile;
  @Uint64()
  external int ullTotalVirtual;
  @Uint64()
  external int ullAvailVirtual;
  @Uint64()
  external int ullAvailExtendedVirtual;
}

typedef _GlobalMemoryStatusExNative = Int32 Function(Pointer<_MemoryStatusEx>);
typedef _GlobalMemoryStatusExDart = int Function(Pointer<_MemoryStatusEx>);

// Top-level finals are lazily initialised in Dart, so nothing is opened on
// platforms that never reach this code path.
final _GlobalMemoryStatusExDart _globalMemoryStatusEx =
    DynamicLibrary.open('kernel32.dll').lookupFunction<
        _GlobalMemoryStatusExNative,
        _GlobalMemoryStatusExDart>('GlobalMemoryStatusEx');

/// Reports physical memory. A Job Object memory limit, which is how Windows
/// containers cap a process tree, is not reflected here — the equivalent of
/// the cgroup handling on Linux would need `QueryInformationJobObject`.
MemoryInfo _windowsMemoryInfo() {
  final status = calloc<_MemoryStatusEx>();
  try {
    // dwLength must be set before the call; the API uses it to identify the
    // structure version.
    status.ref.dwLength = sizeOf<_MemoryStatusEx>();
    if (_globalMemoryStatusEx(status) == 0) {
      throw StateError('GlobalMemoryStatusEx failed');
    }
    return MemoryInfo(
      total: status.ref.ullTotalPhys,
      available: status.ref.ullAvailPhys,
      totalLimitedBy: MemoryLimitSource.host,
    );
  } finally {
    calloc.free(status);
  }
}

// ---------------------------------------------------------------------------
// macOS — sysctlbyname + host_statistics64 (libSystem)
// ---------------------------------------------------------------------------

/// `struct vm_statistics64` from <mach/vm_statistics.h>.
///
/// `natural_t` is 32 bits; the cumulative counters are 64. Field order and
/// width must match the C header exactly — a misaligned field yields nonsense
/// values without crashing.
final class _VMStatistics64 extends Struct {
  @Uint32()
  external int freeCount;
  @Uint32()
  external int activeCount;
  @Uint32()
  external int inactiveCount;
  @Uint32()
  external int wireCount;
  @Uint64()
  external int zeroFillCount;
  @Uint64()
  external int reactivations;
  @Uint64()
  external int pageIns;
  @Uint64()
  external int pageOuts;
  @Uint64()
  external int faults;
  @Uint64()
  external int cowFaults;
  @Uint64()
  external int lookups;
  @Uint64()
  external int hits;
  @Uint64()
  external int purges;
  @Uint32()
  external int purgeableCount;
  @Uint32()
  external int speculativeCount;
  @Uint64()
  external int decompressions;
  @Uint64()
  external int compressions;
  @Uint64()
  external int swapIns;
  @Uint64()
  external int swapOuts;
  @Uint32()
  external int compressorPageCount;
  @Uint32()
  external int throttledCount;
  @Uint32()
  external int externalPageCount;
  @Uint32()
  external int internalPageCount;
  @Uint64()
  external int totalUncompressedPagesInCompressor;
}

const int _hostVmInfo64 = 4; // HOST_VM_INFO64

typedef _MachHostSelfNative = Uint32 Function();
typedef _MachHostSelfDart = int Function();

typedef _HostStatistics64Native = Int32 Function(
    Uint32 host, Int32 flavor, Pointer<Uint32> info, Pointer<Uint32> count);
typedef _HostStatistics64Dart = int Function(
    int host, int flavor, Pointer<Uint32> info, Pointer<Uint32> count);

typedef _SysctlByNameNative = Int32 Function(Pointer<Utf8> name,
    Pointer<Void> oldp, Pointer<Size> oldlenp, Pointer<Void> newp, Size newlen);
typedef _SysctlByNameDart = int Function(Pointer<Utf8> name, Pointer<Void> oldp,
    Pointer<Size> oldlenp, Pointer<Void> newp, int newlen);

final DynamicLibrary _libSystem = DynamicLibrary.process();

final _MachHostSelfDart _machHostSelf = _libSystem
    .lookupFunction<_MachHostSelfNative, _MachHostSelfDart>('mach_host_self');

final _HostStatistics64Dart _hostStatistics64 =
    _libSystem.lookupFunction<_HostStatistics64Native, _HostStatistics64Dart>(
        'host_statistics64');

final _SysctlByNameDart _sysctlByName = _libSystem
    .lookupFunction<_SysctlByNameNative, _SysctlByNameDart>('sysctlbyname');

MemoryInfo _macosMemoryInfo() {
  // hw.memsize is ALREADY in bytes. The well-known system_info2/3 bug comes
  // from a superfluous multiplication by the page size right here.
  final total = _sysctlUint64('hw.memsize');

  final stats = calloc<_VMStatistics64>();
  final count = calloc<Uint32>();
  try {
    // HOST_VM_INFO64_COUNT is sizeof(struct) / sizeof(integer_t). Computing it
    // avoids hardcoding a value that moves with the header, and pins the
    // struct revision being requested: a newer kernel fills only the fields we
    // asked for. A kernel older than this revision fails the call outright,
    // which is caught below.
    count.value = sizeOf<_VMStatistics64>() ~/ sizeOf<Uint32>();

    final result = _hostStatistics64(
        _machHostSelf(), _hostVmInfo64, stats.cast<Uint32>(), count);
    if (result != 0) {
      throw StateError('host_statistics64 failed (kern_return_t = $result)');
    }

    final pageSize = _macosPageSize();
    final s = stats.ref;

    // Mirrors Activity Monitor's breakdown:
    //   used = app memory + wired + compressed
    // where app memory is the non-purgeable internal page count.
    final usedPages = (s.internalPageCount - s.purgeableCount) +
        s.wireCount +
        s.compressorPageCount;
    final used = usedPages * pageSize;

    return MemoryInfo(
      total: total,
      available: math.max(0, math.min(total, total - used)),
      totalLimitedBy: MemoryLimitSource.host,
    );
  } finally {
    calloc.free(stats);
    calloc.free(count);
  }
}

/// Page size as the kernel means it for the counters above.
///
/// 4096 on Intel, 16384 on Apple silicon — and an x86 binary under Rosetta
/// sees 4096 from the process's point of view while the kernel counts in
/// 16384. Hence `vm_kernel_page_size`, exported by libSystem, rather than
/// `sysconf(_SC_PAGESIZE)`, which reflects the process.
int _macosPageSize() {
  try {
    return _libSystem.lookup<Uint64>('vm_kernel_page_size').value;
  } on ArgumentError {
    return _sysctlUint64('hw.pagesize');
  }
}

int _sysctlUint64(String name) {
  final namePtr = name.toNativeUtf8();
  final out = calloc<Uint64>();
  final length = calloc<Size>();
  try {
    length.value = sizeOf<Uint64>();
    if (_sysctlByName(namePtr, out.cast<Void>(), length, nullptr, 0) != 0) {
      throw StateError('sysctlbyname("$name") failed');
    }
    return out.value;
  } finally {
    calloc.free(namePtr);
    calloc.free(out);
    calloc.free(length);
  }
}