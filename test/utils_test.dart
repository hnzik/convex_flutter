import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:convex_flutter/src/utils.dart';

void main() {
  group('buildArgs', () {
    test('encodes string values as JSON strings', () {
      final result = buildArgs({'name': 'John'});

      expect(result, {'name': '"John"'});
      expect(jsonDecode(result['name']!), 'John');
    });

    test('encodes integer values as JSON', () {
      final result = buildArgs({'count': 42});

      expect(result, {'count': '42'});
      expect(jsonDecode(result['count']!), 42);
    });

    test('encodes double values as JSON', () {
      final result = buildArgs({'price': 19.99});

      expect(result, {'price': '19.99'});
      expect(jsonDecode(result['price']!), 19.99);
    });

    test('encodes boolean values as JSON', () {
      final result = buildArgs({'active': true, 'deleted': false});

      expect(result, {'active': 'true', 'deleted': 'false'});
      expect(jsonDecode(result['active']!), true);
      expect(jsonDecode(result['deleted']!), false);
    });

    test('encodes null values as JSON', () {
      final result = buildArgs({'value': null});

      expect(result, {'value': 'null'});
      expect(jsonDecode(result['value']!), null);
    });

    test('encodes list values as JSON arrays', () {
      final result = buildArgs({
        'items': [1, 2, 3]
      });

      expect(result, {'items': '[1,2,3]'});
      expect(jsonDecode(result['items']!), [1, 2, 3]);
    });

    test('encodes nested objects as JSON', () {
      final result = buildArgs({
        'user': {'name': 'John', 'age': 30}
      });

      expect(result, {'user': '{"name":"John","age":30}'});
      expect(jsonDecode(result['user']!), {'name': 'John', 'age': 30});
    });

    test('handles empty map', () {
      final result = buildArgs({});

      expect(result, isEmpty);
    });

    test('handles multiple mixed-type values', () {
      final result = buildArgs({
        'string': 'hello',
        'number': 123,
        'bool': true,
        'list': [1, 'two'],
        'nested': {'key': 'value'},
      });

      expect(result.length, 5);
      expect(jsonDecode(result['string']!), 'hello');
      expect(jsonDecode(result['number']!), 123);
      expect(jsonDecode(result['bool']!), true);
      expect(jsonDecode(result['list']!), [1, 'two']);
      expect(jsonDecode(result['nested']!), {'key': 'value'});
    });

    test('handles special characters in strings', () {
      final result = buildArgs({'text': 'Hello "World"\nNew Line'});

      // The result should be a valid JSON string
      final decoded = jsonDecode(result['text']!);
      expect(decoded, 'Hello "World"\nNew Line');
    });

    test('handles unicode characters', () {
      final result = buildArgs({'emoji': '👋🌍'});

      expect(jsonDecode(result['emoji']!), '👋🌍');
    });

    test('handles deeply nested structures', () {
      final result = buildArgs({
        'deep': {
          'level1': {
            'level2': {
              'level3': 'value'
            }
          }
        }
      });

      final decoded = jsonDecode(result['deep']!);
      expect(decoded['level1']['level2']['level3'], 'value');
    });
  });
}
