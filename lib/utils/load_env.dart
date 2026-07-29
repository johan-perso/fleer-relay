import 'package:dotenv/dotenv.dart';

final env = DotEnv(includePlatformEnvironment: true)..load(['.env']);

dynamic getValueFromEnv({ required String key, required dynamic fallback, required Type type }) {
  var value = env[key];
  if (value == null) {
    return fallback;
  }

  if (value.runtimeType != type) {
    switch (type) {
      case int:
        return int.tryParse(value) ?? fallback;
      case double:
        return double.tryParse(value) ?? fallback;
      case bool:
        final lowerValue = value.toLowerCase().trim();
        if (lowerValue == 'true') return true;
        if (lowerValue == 'false') return false;
        return fallback;
      default:
        return value;
    }
  }

  return value;
}