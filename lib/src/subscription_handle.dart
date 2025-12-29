/// A handle to manage a real-time subscription to a Convex query.
///
/// Use [cancel] to stop receiving updates and clean up resources.
abstract class SubscriptionHandle {
  /// Cancels the subscription and stops receiving updates.
  void cancel();
}
