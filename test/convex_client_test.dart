import 'package:flutter_test/flutter_test.dart';

import 'mock_convex_client.dart';

void main() {
  group('MockConvexClient', () {
    late MockConvexClient client;

    setUp(() {
      client = MockConvexClient();
    });

    group('query', () {
      test('records query calls', () async {
        await client.query('messages:list', {'channel': '"general"'});

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'query');
        expect(client.lastCall?.arguments['name'], 'messages:list');
        expect(client.lastCall?.arguments['args'], {'channel': '"general"'});
      });

      test('returns configured response', () async {
        client.queryResponses['users:get'] = '{"id": "123", "name": "John"}';

        final result = await client.query('users:get', {'id': '"123"'});

        expect(result, '{"id": "123", "name": "John"}');
      });

      test('returns default response when not configured', () async {
        final result = await client.query('unknown:query', {});

        expect(result, '[]');
      });

      test('can customize default response', () async {
        client.defaultQueryResponse = '{"default": true}';

        final result = await client.query('any:query', {});

        expect(result, '{"default": true}');
      });
    });

    group('mutation', () {
      test('records mutation calls', () async {
        await client.mutation(
          name: 'messages:send',
          args: {'text': 'Hello'},
        );

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'mutation');
        expect(client.lastCall?.arguments['name'], 'messages:send');
        expect(client.lastCall?.arguments['args'], {'text': 'Hello'});
      });

      test('returns configured response', () async {
        client.mutationResponses['messages:send'] = '"msg_123"';

        final result = await client.mutation(
          name: 'messages:send',
          args: {'text': 'Hello'},
        );

        expect(result, '"msg_123"');
      });
    });

    group('action', () {
      test('records action calls', () async {
        await client.action(
          name: 'ai:generate',
          args: {'prompt': 'Hello'},
        );

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'action');
        expect(client.lastCall?.arguments['name'], 'ai:generate');
      });

      test('returns configured response', () async {
        client.actionResponses['ai:generate'] = '"Generated text"';

        final result = await client.action(
          name: 'ai:generate',
          args: {'prompt': 'Hello'},
        );

        expect(result, '"Generated text"');
      });
    });

    group('subscribe', () {
      test('records subscribe calls', () async {
        await client.subscribe(
          name: 'messages:list',
          args: {'channel': '"general"'},
          onUpdate: (_) {},
          onError: (_, __) {},
        );

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'subscribe');
        expect(client.lastCall?.arguments['name'], 'messages:list');
      });

      test('returns a subscription handle', () async {
        final handle = await client.subscribe(
          name: 'messages:list',
          args: {},
          onUpdate: (_) {},
          onError: (_, __) {},
        );

        expect(handle, isA<MockSubscriptionHandle>());
      });
    });

    group('setAuth', () {
      test('records setAuth calls with token', () async {
        await client.setAuth(token: 'jwt_token_123');

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'setAuth');
        expect(client.lastCall?.arguments['token'], 'jwt_token_123');
        expect(client.currentAuthToken, 'jwt_token_123');
      });

      test('records setAuth calls with null token', () async {
        await client.setAuth(token: null);

        expect(client.calls.length, 1);
        expect(client.lastCall?.method, 'setAuth');
        expect(client.lastCall?.arguments['token'], null);
        expect(client.currentAuthToken, null);
      });
    });

    group('reset', () {
      test('clears all calls', () async {
        await client.query('test', {});
        await client.mutation(name: 'test', args: {});

        client.reset();

        expect(client.calls, isEmpty);
      });

      test('clears auth token', () async {
        await client.setAuth(token: 'token');
        client.reset();

        expect(client.currentAuthToken, null);
      });
    });

    group('callsTo', () {
      test('filters calls by method', () async {
        await client.query('q1', {});
        await client.mutation(name: 'm1', args: {});
        await client.query('q2', {});

        final queryCalls = client.callsTo('query');

        expect(queryCalls.length, 2);
        expect(queryCalls[0].arguments['name'], 'q1');
        expect(queryCalls[1].arguments['name'], 'q2');
      });
    });
  });

  group('MockSubscriptionHandle', () {
    test('starts not cancelled', () {
      final handle = MockSubscriptionHandle(
        onUpdate: (_) {},
        onError: (_, __) {},
      );

      expect(handle.isCancelled, false);
    });

    test('cancel sets cancelled flag', () {
      final handle = MockSubscriptionHandle(
        onUpdate: (_) {},
        onError: (_, __) {},
      );

      handle.cancel();

      expect(handle.isCancelled, true);
    });

    test('simulateUpdate calls onUpdate callback', () {
      String? receivedData;
      final handle = MockSubscriptionHandle(
        onUpdate: (data) => receivedData = data,
        onError: (_, __) {},
      );

      handle.simulateUpdate('{"message": "Hello"}');

      expect(receivedData, '{"message": "Hello"}');
    });

    test('simulateUpdate does not call callback after cancel', () {
      String? receivedData;
      final handle = MockSubscriptionHandle(
        onUpdate: (data) => receivedData = data,
        onError: (_, __) {},
      );

      handle.cancel();
      handle.simulateUpdate('{"message": "Hello"}');

      expect(receivedData, null);
    });

    test('simulateError calls onError callback', () {
      String? receivedMessage;
      String? receivedData;
      final handle = MockSubscriptionHandle(
        onUpdate: (_) {},
        onError: (msg, data) {
          receivedMessage = msg;
          receivedData = data;
        },
      );

      handle.simulateError('Connection failed', '{"code": 500}');

      expect(receivedMessage, 'Connection failed');
      expect(receivedData, '{"code": 500}');
    });

    test('simulateError does not call callback after cancel', () {
      String? receivedMessage;
      final handle = MockSubscriptionHandle(
        onUpdate: (_) {},
        onError: (msg, _) => receivedMessage = msg,
      );

      handle.cancel();
      handle.simulateError('Connection failed');

      expect(receivedMessage, null);
    });
  });
}
