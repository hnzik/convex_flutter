import 'dart:async';
import 'dart:convert';
import 'dart:js_interop';

import 'convex_client_interface.dart';
import 'subscription_handle.dart';

/// JS interop bindings for the Convex browser client.
///
/// These bindings correspond to the ConvexClient class from convex-js.
/// Users must include the convex browser bundle in their web/index.html:
/// <script src="https://unpkg.com/convex@latest/dist/browser.bundle.js"></script>
@JS('convex.ConvexClient')
extension type JSConvexClient._(JSObject _) implements JSObject {
  external factory JSConvexClient(String address);

  external JSPromise<JSAny?> query(JSAny name, JSAny? args);
  external JSPromise<JSAny?> mutation(JSAny name, JSAny? args);
  external JSPromise<JSAny?> action(JSAny name, JSAny? args);

  /// Subscribe to real-time updates. Returns an unsubscribe function.
  external JSFunction onUpdate(JSAny query, JSAny? args, JSFunction callback);

  /// Set authentication using a token fetcher function.
  external void setAuth(JSFunction tokenFetcher, JSFunction? onChange);

  /// Clear authentication.
  external void clearAuth();

  /// Close the client connection.
  external JSPromise<JSAny?> close();
}

/// Check if ConvexClient is defined on window.convex namespace.
@JS('window.convex.ConvexClient')
external JSFunction? get _convexClientConstructor;

/// Convert a Dart object to a JSON string and parse it to JS.
@JS('JSON.parse')
external JSAny? _jsonParse(String json);

/// Convert a JS value to a JSON string.
@JS('JSON.stringify')
external String? _jsonStringify(JSAny? value);

/// Web (JS interop) implementation of the Convex client.
///
/// This implementation uses dart:js_interop to communicate with
/// the Convex JavaScript browser library.
class WebConvexClient implements ConvexClientInterface {
  final JSConvexClient _client;
  String? _currentToken;

  WebConvexClient._(this._client);

  /// Creates a new web Convex client.
  ///
  /// Requires the convex browser bundle to be loaded in the page.
  static Future<WebConvexClient> create(
    String deploymentUrl,
    String clientId,
  ) async {
    // Check if ConvexClient is available
    if (_convexClientConstructor == null) {
      throw StateError(
        'ConvexClient not found. Please include the Convex browser bundle in your web/index.html:\n'
        '<script src="https://unpkg.com/convex@latest/dist/browser.bundle.js"></script>',
      );
    }

    final client = JSConvexClient(deploymentUrl);
    return WebConvexClient._(client);
  }

  /// Convert a Dart Map to a JS object by going through JSON.
  /// Strips null values since Convex expects optional fields to be omitted.
  /// Returns an empty JS object if all values are null/stripped.
  JSAny _mapToJS(Map<String, dynamic> map) {
    final stripped = _stripNulls(map);
    final jsonString = jsonEncode(stripped);
    return _jsonParse(jsonString)!;
  }

  /// Recursively strip null values from a map.
  /// Convex doesn't accept null for optional fields - they should be omitted.
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

  /// Convert a JSAny result to a JSON string.
  String _jsToJsonString(JSAny? value) {
    if (value == null) return 'null';
    final result = _jsonStringify(value);
    return result ?? 'null';
  }

  /// Convert function path from native format to JS format.
  ///
  /// Native format: `module:submodule:functionName`
  /// JS format: `module/submodule:functionName`
  ///
  /// All colons except the last one become slashes.
  String _convertFunctionPath(String name) {
    final lastColon = name.lastIndexOf(':');
    if (lastColon == -1) return name;

    final modulePath = name.substring(0, lastColon).replaceAll(':', '/');
    final functionName = name.substring(lastColon + 1);
    return '$modulePath:$functionName';
  }

  @override
  Future<String> query(String name, Map<String, dynamic> args) async {
    final jsArgs = _mapToJS(args);
    final jsName = _convertFunctionPath(name);

    try {
      final result = await _client.query(jsName.toJS, jsArgs).toDart;
      return _jsToJsonString(result);
    } catch (e) {
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
    final jsArgs = _mapToJS(args);
    final jsName = _convertFunctionPath(name);

    // Create a JS callback that calls our Dart onUpdate function
    void dartCallback(JSAny? result) {
      try {
        final jsonResult = _jsToJsonString(result);
        onUpdate(jsonResult);
      } catch (e) {
        onError(e.toString(), null);
      }
    }

    final jsCallback = dartCallback.toJS;

    // onUpdate returns an unsubscribe function
    final unsubscribe = _client.onUpdate(jsName.toJS, jsArgs, jsCallback);

    return WebSubscriptionHandle(unsubscribe);
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final jsArgs = _mapToJS(args);
    final jsName = _convertFunctionPath(name);

    try {
      final result = await _client.mutation(jsName.toJS, jsArgs).toDart;
      return _jsToJsonString(result);
    } catch (e) {
      throw _convertError(e);
    }
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final jsArgs = _mapToJS(args);
    final jsName = _convertFunctionPath(name);

    try {
      final result = await _client.action(jsName.toJS, jsArgs).toDart;
      return _jsToJsonString(result);
    } catch (e) {
      throw _convertError(e);
    }
  }

  @override
  Future<void> setAuth({required String? token}) async {
    _currentToken = token;

    if (token == null) {
      _client.clearAuth();
    } else {
      // Create a token fetcher function that returns the token
      // Parameter is JSAny? to avoid type conversion issues with JS booleans
      JSAny? tokenFetcher(JSAny? forceRefresh) {
        return _currentToken?.toJS;
      }

      _client.setAuth(tokenFetcher.toJS, null);
    }
  }

  /// Convert an error to a Dart exception.
  Exception _convertError(Object error) {
    return Exception(error.toString());
  }
}

/// Web implementation of SubscriptionHandle.
class WebSubscriptionHandle implements SubscriptionHandle {
  final JSFunction _unsubscribe;

  WebSubscriptionHandle(this._unsubscribe);

  @override
  void cancel() {
    _unsubscribe.callAsFunction();
  }
}

/// Factory function for creating a platform-specific client.
///
/// This is called by the conditional import mechanism.
Future<ConvexClientInterface> createPlatformClient(
  String deploymentUrl,
  String clientId,
) {
  return WebConvexClient.create(deploymentUrl, clientId);
}
