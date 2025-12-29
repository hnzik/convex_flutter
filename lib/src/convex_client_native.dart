import 'convex_client_interface.dart';
import 'subscription_handle.dart';
import 'utils.dart';
import 'rust/lib.dart' as rust;
import 'rust/frb_generated.dart';

/// Native (Rust FFI) implementation of the Convex client.
///
/// This implementation uses flutter_rust_bridge to communicate with
/// the Convex backend via the Rust convex crate.
class NativeConvexClient implements ConvexClientInterface {
  final rust.MobileConvexClient _client;

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
    return await _client.query(name: name, args: formattedArgs);
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
    return await _client.mutation(name: name, args: formattedArgs);
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final formattedArgs = buildArgs(args);
    return await _client.action(name: name, args: formattedArgs);
  }

  @override
  Future<void> setAuth({required String? token}) async {
    return await _client.setAuth(token: token);
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
