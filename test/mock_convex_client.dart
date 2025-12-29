import 'package:convex_flutter/src/convex_client_interface.dart';
import 'package:convex_flutter/src/subscription_handle.dart';

/// A mock implementation of [ConvexClientInterface] for testing.
///
/// This mock tracks all method calls and allows configuring responses.
class MockConvexClient implements ConvexClientInterface {
  /// List of all method calls made to this mock.
  final List<MockCall> calls = [];

  /// Configured responses for query calls.
  final Map<String, String> queryResponses = {};

  /// Configured responses for mutation calls.
  final Map<String, String> mutationResponses = {};

  /// Configured responses for action calls.
  final Map<String, String> actionResponses = {};

  /// The current auth token.
  String? currentAuthToken;

  /// Default response for queries.
  String defaultQueryResponse = '[]';

  /// Default response for mutations.
  String defaultMutationResponse = 'null';

  /// Default response for actions.
  String defaultActionResponse = 'null';

  @override
  Future<String> query(String name, Map<String, dynamic> args) async {
    calls.add(MockCall('query', {'name': name, 'args': args}));
    return queryResponses[name] ?? defaultQueryResponse;
  }

  @override
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  }) async {
    calls.add(MockCall('subscribe', {
      'name': name,
      'args': args,
    }));
    return MockSubscriptionHandle(
      onUpdate: onUpdate,
      onError: onError,
    );
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    calls.add(MockCall('mutation', {'name': name, 'args': args}));
    return mutationResponses[name] ?? defaultMutationResponse;
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    calls.add(MockCall('action', {'name': name, 'args': args}));
    return actionResponses[name] ?? defaultActionResponse;
  }

  @override
  Future<void> setAuth({required String? token}) async {
    calls.add(MockCall('setAuth', {'token': token}));
    currentAuthToken = token;
  }

  /// Clears all recorded calls.
  void reset() {
    calls.clear();
    currentAuthToken = null;
  }

  /// Returns the last call made to this mock, or null if no calls were made.
  MockCall? get lastCall => calls.isNotEmpty ? calls.last : null;

  /// Returns all calls to a specific method.
  List<MockCall> callsTo(String method) =>
      calls.where((c) => c.method == method).toList();
}

/// Represents a single method call to the mock.
class MockCall {
  final String method;
  final Map<String, dynamic> arguments;

  MockCall(this.method, this.arguments);

  @override
  String toString() => 'MockCall($method, $arguments)';
}

/// A mock subscription handle for testing.
class MockSubscriptionHandle implements SubscriptionHandle {
  final void Function(String) onUpdate;
  final void Function(String, String?) onError;

  bool _cancelled = false;

  MockSubscriptionHandle({
    required this.onUpdate,
    required this.onError,
  });

  @override
  void cancel() {
    _cancelled = true;
  }

  /// Whether this subscription has been cancelled.
  bool get isCancelled => _cancelled;

  /// Simulate receiving an update.
  void simulateUpdate(String data) {
    if (!_cancelled) {
      onUpdate(data);
    }
  }

  /// Simulate receiving an error.
  void simulateError(String message, [String? data]) {
    if (!_cancelled) {
      onError(message, data);
    }
  }
}
