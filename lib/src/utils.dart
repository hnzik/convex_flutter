import 'dart:convert';

/// Build args map for native client, stripping null values.
/// Convex doesn't accept null for optional fields - they should be omitted.
Map<String, String> buildArgs(Map<String, dynamic> record) {
  final stripped = _stripNulls(record);
  return {for (var entry in stripped.entries) entry.key: jsonEncode(entry.value)};
}

/// Recursively strip null values from a map.
Map<String, dynamic> _stripNulls(Map<String, dynamic> map) {
  final result = <String, dynamic>{};
  for (final entry in map.entries) {
    final value = entry.value;
    if (value != null) {
      if (value is Map<String, dynamic>) {
        result[entry.key] = _stripNulls(value);
      } else if (value is List) {
        result[entry.key] = _stripNullsFromList(value);
      } else {
        result[entry.key] = value;
      }
    }
  }
  return result;
}

/// Recursively strip null values from list items.
List<dynamic> _stripNullsFromList(List<dynamic> list) {
  return list.map((item) {
    if (item is Map<String, dynamic>) {
      return _stripNulls(item);
    } else if (item is List) {
      return _stripNullsFromList(item);
    }
    return item;
  }).toList();
}
