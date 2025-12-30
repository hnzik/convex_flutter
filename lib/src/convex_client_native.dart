import 'convex_client_interface.dart';
import 'subscription_handle.dart';
import 'utils.dart';
import 'rust/lib.dart' as rust;
import 'rust/lib.dart'
    show
        ClientError,
        ClientError_InternalError,
        ClientError_ConvexError,
        ClientError_ServerError;
import 'rust/frb_generated.dart';

/// Native (Rust FFI) implementation of the Convex client.
///
/// This implementation uses flutter_rust_bridge to communicate with
/// the Convex backend via the Rust convex crate.
class NativeConvexClient implements ConvexClientInterface {
  final rust.MobileConvexClient _client;
  rust.AuthErrorStreamReceiver? _authErrorReceiver;
  bool _authErrorListenerRunning = false;

  NativeConvexClient._(this._client);

  /// Creates a new native Convex client.
  ///
  /// Initializes the Rust FFI library and creates a new client instance.
  static Future<NativeConvexClient> create(
    String deploymentUrl,
    String clientId,
  ) async {
    await RustLib.init();
    final client = rust.MobileConvexClient(
      deploymentUrl: deploymentUrl,
      clientId: clientId,
    );
    return NativeConvexClient._(client);
  }

  @override
  Future<String> query(String name, Map<String, dynamic> args) async {
    final formattedArgs = buildArgs(args);
    try {
      return await _client.query(name: name, args: formattedArgs);
    } on ClientError catch (e) {
      throw _convertError(e);
    }
  }

  @override
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  }) async {
    final formattedArgs = buildArgs(args);
    final handle = await _client.subscribe(
      name: name,
      args: formattedArgs,
      onUpdate: (value) => onUpdate(value),
      onError: (message, value) => onError(message, value),
    );

    return NativeSubscriptionHandle(handle);
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final formattedArgs = buildArgs(args);
    try {
      return await _client.mutation(name: name, args: formattedArgs);
    } on ClientError catch (e) {
      throw _convertError(e);
    }
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final formattedArgs = buildArgs(args);
    try {
      return await _client.action(name: name, args: formattedArgs);
    } on ClientError catch (e) {
      throw _convertError(e);
    }
  }

  @override
  Future<void> setAuth({required String? token}) async {
    return await _client.setAuth(token: token);
  }

  @override
  void setAuthErrorHandler(AuthErrorCallback callback) {
    if (_authErrorListenerRunning) {
      return; // Already listening
    }
    _authErrorListenerRunning = true;

    // Start listening for auth errors in the background
    _startAuthErrorListener(callback);
  }

  Future<void> _startAuthErrorListener(AuthErrorCallback callback) async {
    // Register the handler and get the stream receiver
    _authErrorReceiver ??= await _client.registerAuthErrorHandler();

    // Listen for auth errors in a loop
    while (_authErrorListenerRunning) {
      final error = await _authErrorReceiver!.recv();
      if (error == null) {
        // Stream closed
        _authErrorListenerRunning = false;
        break;
      }

      // Call the user's callback to get the action
      final action = await callback(error);

      // Send the action back to Rust
      _client.respondToAuthError(action: action);
    }
  }

  /// Convert a ClientError to a Dart exception.
  Exception _convertError(ClientError error) {
    return switch (error) {
      ClientError_InternalError(:final msg) => Exception('InternalError: $msg'),
      ClientError_ConvexError(:final data) => Exception('ConvexError: $data'),
      ClientError_ServerError(:final msg) => Exception('ServerError: $msg'),
    };
  }
}

/// Native implementation of SubscriptionHandle wrapping the Rust handle.
class NativeSubscriptionHandle implements SubscriptionHandle {
  final rust.SubscriptionHandle _handle;

  NativeSubscriptionHandle(this._handle);

  @override
  void cancel() {
    _handle.cancel();
  }
}

/// Factory function for creating a platform-specific client.
///
/// This is called by the conditional import mechanism.
Future<ConvexClientInterface> createPlatformClient(
  String deploymentUrl,
  String clientId,
) {
  return NativeConvexClient.create(deploymentUrl, clientId);
}
