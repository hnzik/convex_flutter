import 'subscription_handle.dart';
import 'rust/lib.dart' show AuthError, AuthErrorAction;

/// Callback type for handling auth errors.
/// Returns an [AuthErrorAction] indicating how to respond to the error.
typedef AuthErrorCallback = Future<AuthErrorAction> Function(AuthError error);

/// Abstract interface for Convex client implementations.
///
/// This interface defines the contract that both native (Rust FFI) and web
/// (JS interop) implementations must follow.
abstract class ConvexClientInterface {
  /// Executes a Convex query operation.
  ///
  /// [name] - Name of the query function to execute
  /// [args] - Map of arguments to pass to the query (null values are stripped)
  ///
  /// Returns the query result as a JSON string
  Future<String> query(String name, Map<String, dynamic> args);

  /// Creates a real-time subscription to a Convex query.
  ///
  /// [name] - Name of the query function to subscribe to
  /// [args] - Map of arguments for the subscription (null values are stripped)
  /// [onUpdate] - Callback function called when new data arrives
  /// [onError] - Callback function called when an error occurs
  ///
  /// Returns a handle that can be used to cancel the subscription
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  });

  /// Executes a Convex mutation operation.
  ///
  /// [name] - Name of the mutation function to execute
  /// [args] - Map of arguments to pass to the mutation
  ///
  /// Returns the mutation result as a JSON string
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  });

  /// Executes a Convex action operation.
  ///
  /// [name] - Name of the action function to execute
  /// [args] - Map of arguments to pass to the action
  ///
  /// Returns the action result as a JSON string
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  });

  /// Sets the authentication token for the client.
  ///
  /// [token] - The authentication token to set, or null to clear
  Future<void> setAuth({required String? token});

  /// Registers a callback to handle authentication errors from the backend.
  ///
  /// When the Convex backend rejects an auth token (e.g., expired or invalid),
  /// the callback will be invoked with the [AuthError] details.
  /// The callback should return an [AuthErrorAction] to specify how to proceed:
  /// - [AuthErrorAction.refreshToken]: Provide a new token to retry
  /// - [AuthErrorAction.clearAuth]: Clear authentication and continue anonymously
  /// - [AuthErrorAction.disconnect]: Disconnect the client entirely
  ///
  /// Note: The callback should respond within 30 seconds, otherwise the client
  /// will default to [AuthErrorAction.clearAuth].
  void setAuthErrorHandler(AuthErrorCallback callback);
}
