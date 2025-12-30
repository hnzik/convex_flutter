import 'dart:async';
import 'dart:convert';

import 'convex_client_interface.dart';
import 'subscription_handle.dart';
import 'rust/lib.dart' show AuthError, AuthErrorAction;

// Conditional imports for platform-specific implementations
import 'convex_client_stub.dart'
    if (dart.library.io) 'convex_client_native.dart'
    if (dart.library.js_interop) 'convex_client_web.dart';

// Re-export auth error types for convenience
export 'rust/lib.dart' show AuthError, AuthErrorAction;

/// A client for interacting with a Convex backend service.
///
/// The ConvexClient provides methods for executing queries, mutations, actions and
/// managing real-time subscriptions with a Convex backend.
///
/// This client automatically uses the appropriate implementation based on the platform:
/// - Native platforms (iOS, Android, macOS, Windows, Linux): Uses Rust FFI
/// - Web: Uses JavaScript interop with the convex-js library
///
/// Example usage:
///
/// ```dart
/// // Initialize the client
/// final client = await ConvexClient.init(
///   deploymentUrl: "https://my-app.convex.cloud",
///   clientId: "flutter-app-1.0"
/// );
///
/// // Execute a query
/// final result = await client.query(
///   "messages:list",
///   {"limit": "10"}
/// );
///
/// // Subscribe to real-time updates
/// final subscription = await client.subscribe(
///   name: "messages:list",
///   args: {},
///   onUpdate: (value) {
///     print("New messages: $value");
///   },
///   onError: (message, value) {
///     print("Error: $message");
///   }
/// );
///
/// // Execute a mutation
/// await client.mutation(
///   name: "messages:send",
///   args: {
///     "body": "Hello!",
///     "author": "User123"
///   }
/// );
///
/// // Cancel subscription when done
/// subscription.cancel();
/// ```
///
/// ## Web Setup
///
/// For web support, include the Convex browser bundle in your `web/index.html`:
///
/// ```html
/// <script src="https://unpkg.com/convex@latest/dist/browser.bundle.js"></script>
/// ```
class ConvexClient {
  /// Private static instance for singleton pattern
  static ConvexClient? _instance;
  static Future<ConvexClient>? _initFuture;
  static String? _initializedDeploymentUrl;
  static String? _initializedClientId;

  final StreamController<AuthExpiredException> _authExpiredController =
      StreamController<AuthExpiredException>.broadcast();
  Timer? _authExpiryTimer;
  DateTime? _authTokenExpiresAt;
  bool _authExpiredNotified = false;

  /// The underlying platform-specific client
  late final ConvexClientInterface _client;

  /// Public getter to access singleton instance
  /// Throws if accessed before initialization
  static ConvexClient get instance => _instance!;

  /// Stream that emits an AuthExpiredException when the current auth token expires.
  Stream<AuthExpiredException> get authExpired => _authExpiredController.stream;

  /// Initializes the ConvexClient singleton instance
  ///
  /// [deploymentUrl] - The URL of your Convex deployment
  /// [clientId] - A unique identifier for this client instance
  ///
  /// Returns the singleton instance after initialization
  /// Will reuse existing instance if already initialized
  static Future<ConvexClient> init({
    required String deploymentUrl,
    required String clientId,
  }) async {
    if (_instance != null) {
      _assertMatchingConfig(deploymentUrl, clientId);
      return _instance!;
    }
    if (_initFuture != null) {
      _assertMatchingConfig(deploymentUrl, clientId);
      return _initFuture!;
    }
    _initializedDeploymentUrl = deploymentUrl;
    _initializedClientId = clientId;
    _initFuture = _initialize(deploymentUrl, clientId);
    return _initFuture!;
  }

  static Future<ConvexClient> _initialize(
    String deploymentUrl,
    String clientId,
  ) async {
    try {
      final client = await createPlatformClient(deploymentUrl, clientId);
      _instance = ConvexClient._internal(client);
      return _instance!;
    } catch (e) {
      _initFuture = null;
      _initializedDeploymentUrl = null;
      _initializedClientId = null;
      rethrow;
    }
  }

  static void _assertMatchingConfig(String deploymentUrl, String clientId) {
    if (_initializedDeploymentUrl == null && _initializedClientId == null) {
      return;
    }
    if (_initializedDeploymentUrl != deploymentUrl ||
        _initializedClientId != clientId) {
      throw StateError(
        'ConvexClient.init called more than once with different configuration. '
        'Existing: deploymentUrl=$_initializedDeploymentUrl, '
        'clientId=$_initializedClientId. '
        'New: deploymentUrl=$deploymentUrl, clientId=$clientId.',
      );
    }
  }

  /// Private constructor to prevent direct instantiation
  ConvexClient._internal(this._client);

  /// Executes a Convex query operation
  ///
  /// [name] - Name of the query function to execute
  /// [args] - Map of arguments to pass to the query (null values are stripped)
  ///
  /// Returns the query result as a JSON string
  Future<String> query(String name, Map<String, dynamic> args) async {
    //_assertAuthValid();
    return await _client.query(name, args);
  }

  /// Creates a real-time subscription to a Convex query
  ///
  /// [name] - Name of the query function to subscribe to
  /// [args] - Map of arguments for the subscription (null values are stripped)
  /// [onUpdate] - Callback function called when new data arrives
  /// [onError] - Callback function called when an error occurs
  ///
  /// Returns a handle that can be used to manage the subscription
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  }) async {
    //_assertAuthValid();
    return await _client.subscribe(
      name: name,
      args: args,
      onUpdate: onUpdate,
      onError: (message, value) {
        // Intercept auth expiration errors from native side
        if (message == 'AUTH_EXPIRED') {
          final expiresAt = _authTokenExpiresAt ?? DateTime.now().toUtc();
          _emitAuthExpired(expiresAt);
          return;
        }
        onError(message, value);
      },
    );
  }

  /// Executes a Convex mutation operation
  ///
  /// [name] - Name of the mutation function to execute
  /// [args] - Map of arguments to pass to the mutation
  ///
  /// Returns the mutation result as a JSON string
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    //_assertAuthValid();
    return await _client.mutation(name: name, args: args);
  }

  /// Executes a Convex action operation
  ///
  /// [name] - Name of the action function to execute
  /// [args] - Map of arguments to pass to the action
  ///
  /// Returns the action result as a JSON string
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    //_assertAuthValid();
    return await _client.action(name: name, args: args);
  }

  /// Sets the authentication token for the client
  ///
  /// [token] - The authentication token to set, or null to clear
  ///
  /// Used to authenticate requests to the Convex backend
  Future<void> setAuth({required String? token}) async {
    _updateAuthToken(token);
    return await _client.setAuth(token: token);
  }

  /// Sets a callback to handle authentication errors from the Convex backend.
  ///
  /// When the backend rejects an auth token (e.g., expired or invalid token),
  /// the [callback] will be invoked with the [AuthError] details. The callback
  /// should return an [AuthErrorAction] to specify how to proceed:
  ///
  /// - [AuthErrorAction.refreshToken]: Provide a new token to retry authentication
  /// - [AuthErrorAction.clearAuth]: Clear authentication and continue anonymously
  /// - [AuthErrorAction.disconnect]: Disconnect the client entirely
  ///
  /// Example:
  /// ```dart
  /// client.setAuthErrorHandler((error) async {
  ///   print('Auth error: ${error.errorMessage}');
  ///
  ///   // Try to refresh the token
  ///   final newToken = await myAuthProvider.refreshToken();
  ///   if (newToken != null) {
  ///     return AuthErrorAction.refreshToken(token: newToken);
  ///   }
  ///
  ///   // If refresh fails, clear auth and continue anonymously
  ///   return AuthErrorAction.clearAuth();
  /// });
  /// ```
  ///
  /// Note: The callback should respond within 30 seconds, otherwise the client
  /// will default to [AuthErrorAction.clearAuth].
  void setAuthErrorHandler(AuthErrorCallback callback) {
    _client.setAuthErrorHandler(callback);
  }

  void _updateAuthToken(String? token) {
    _authExpiryTimer?.cancel();
    _authExpiryTimer = null;
    _authTokenExpiresAt = null;
    _authExpiredNotified = false;

    if (token == null) {
      return;
    }

    final expiresAt = _parseJwtExpiration(token);
    if (expiresAt == null) {
      return;
    }

    _authTokenExpiresAt = expiresAt;
    final delay = expiresAt.difference(DateTime.now().toUtc());
    if (delay <= Duration.zero) {
      _emitAuthExpired(expiresAt);
      return;
    }
    _authExpiryTimer = Timer(delay, () {
      _emitAuthExpired(expiresAt);
    });
  }

  void _assertAuthValid() {
    final expiresAt = _authTokenExpiresAt;
    if (expiresAt == null) {
      return;
    }
    if (DateTime.now().toUtc().isAfter(expiresAt)) {
      _emitAuthExpired(expiresAt);
      throw AuthExpiredException(expiresAt);
    }
  }

  void _emitAuthExpired(DateTime expiresAt) {
    if (_authExpiredNotified) {
      return;
    }
    _authExpiredNotified = true;
    _authExpiredController.addError(AuthExpiredException(expiresAt));
  }

  DateTime? _parseJwtExpiration(String token) {
    try {
      final parts = token.split('.');
      if (parts.length < 2) {
        return null;
      }
      final payload = base64Url.normalize(parts[1]);
      final decoded = utf8.decode(base64Url.decode(payload));
      final data = jsonDecode(decoded);
      if (data is! Map<String, dynamic>) {
        return null;
      }
      final exp = data['exp'];
      final expSeconds = exp is int
          ? exp
          : exp is num
          ? exp.toInt()
          : int.tryParse(exp?.toString() ?? '');
      if (expSeconds == null) {
        return null;
      }
      return DateTime.fromMillisecondsSinceEpoch(
        expSeconds * 1000,
        isUtc: true,
      );
    } catch (_) {
      return null;
    }
  }
}

/// Thrown when the current auth token has expired.
class AuthExpiredException implements Exception {
  final DateTime expiredAt;

  AuthExpiredException(this.expiredAt);

  @override
  String toString() => 'AuthExpiredException: token expired at $expiredAt';
}
