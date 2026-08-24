import 'dart:io';
import 'package:system_info3/system_info3.dart';

Future<int> getAvailableRam() async { // in bytes
  if (Platform.isAndroid || Platform.isIOS) {
    return double.infinity.toInt();
  } else {
    final int availableMemory = await SysInfo.getAvailablePhysicalMemory();
    return availableMemory;
  }
}