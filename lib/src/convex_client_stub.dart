import 'convex_client_interface.dart';

/// Stub factory function for unsupported platforms.
///
/// This is used as the default in conditional imports and throws
/// an error if the platform is not supported.
Future<ConvexClientInterface> createPlatformClient(
  String deploymentUrl,
  String clientId,
) {
  throw UnsupportedError(
    'The current platform is not supported. '
    'Convex Flutter supports iOS, Android, macOS, Windows, Linux, and Web.',
  );
}
