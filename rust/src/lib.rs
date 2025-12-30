mod frb_generated;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use once_cell::sync::OnceCell;
use convex::{
    AuthError as ConvexAuthError,
    AuthErrorAction as ConvexAuthErrorAction,
    ConvexClient,
    ConvexClientBuilder,
    FunctionResult,
    Value,
};
use flutter_rust_bridge::{frb, DartFnFuture};
use futures::{
    channel::oneshot::{self, Sender},
    pin_mut, select_biased, FutureExt, StreamExt,
};
use parking_lot::Mutex;
use rustls::crypto::ring;
use tokio::sync::mpsc;

// Custom error type for Convex client operations, exposed to Dart.
#[derive(Debug, thiserror::Error)]
#[frb]
pub enum ClientError {
    /// An internal error within the mobile Convex client.
    #[error("InternalError: {msg}")]
    InternalError { msg: String },
    /// An application-specific error from a remote Convex backend function.
    #[error("ConvexError: {data}")]
    ConvexError { data: String },
    /// An unexpected server-side error from a remote Convex function.
    #[error("ServerError: {msg}")]
    ServerError { msg: String },
}

impl From<anyhow::Error> for ClientError {
    fn from(value: anyhow::Error) -> Self {
        Self::InternalError {
            msg: value.to_string(),
        }
    }
}

/// Authentication error information from the Convex backend.
/// Exposed to Dart when an authentication error occurs.
#[derive(Debug, Clone)]
#[frb]
pub struct AuthError {
    /// The error message describing why authentication failed.
    pub error_message: String,
    /// The base version of the identity that was rejected, if available.
    pub base_version: Option<u32>,
}

impl From<&ConvexAuthError> for AuthError {
    fn from(error: &ConvexAuthError) -> Self {
        AuthError {
            error_message: error.error_message.clone(),
            base_version: error.base_version.map(|v| v as u32),
        }
    }
}

/// Action to take in response to an authentication error.
/// Returned by the auth error callback to tell the client how to proceed.
#[derive(Debug, Clone)]
#[frb]
pub enum AuthErrorAction {
    /// Refresh authentication with a new token.
    RefreshToken { token: String },
    /// Clear authentication and continue as unauthenticated.
    ClearAuth,
    /// Disconnect the client entirely.
    Disconnect,
}

impl From<AuthErrorAction> for ConvexAuthErrorAction {
    fn from(action: AuthErrorAction) -> Self {
        match action {
            AuthErrorAction::RefreshToken { token } => ConvexAuthErrorAction::RefreshToken(token),
            AuthErrorAction::ClearAuth => ConvexAuthErrorAction::ClearAuth,
            AuthErrorAction::Disconnect => ConvexAuthErrorAction::Disconnect,
        }
    }
}

/// Trait defining the interface for handling subscription updates.
// Not directly exposed to Dart, used internally by subscribers.
pub trait QuerySubscriber: Send + Sync {
    fn on_update(&self, value: String); // Called when a new update is received
    fn on_error(&self, message: String, value: Option<String>); // Called on error with optional value
}

/// Adapter struct to implement QuerySubscriber using Dart callbacks.
pub struct CallbackSubscriber {
    on_update: Box<dyn Fn(String) + Send + Sync>, // Callback for updates
    on_error: Box<dyn Fn(String, Option<String>) + Send + Sync>, // Callback for errors
}

impl QuerySubscriber for CallbackSubscriber {
    fn on_update(&self, value: String) {
        (self.on_update)(value);
    }

    fn on_error(&self, message: String, value: Option<String>) {
        (self.on_error)(message, value);
    }
}

/// Opaque type for Dart, representing a subscription handle with cancellation.
#[frb(opaque)]
pub struct SubscriptionHandle {
    cancel_sender: Arc<Mutex<Option<Sender<()>>>>, // Sender to cancel the subscription
}

impl SubscriptionHandle {
    fn new(cancel_sender: Sender<()>) -> Self {
        SubscriptionHandle {
            cancel_sender: Arc::new(Mutex::new(Some(cancel_sender))),
        }
    }

    /// Cancels the subscription by sending a cancellation signal.
    #[frb(sync)]
    pub fn cancel(&self) {
        if let Some(sender) = self.cancel_sender.lock().take() {
            sender.send(()).unwrap();
        }
    }
}

/// Adapter for Dart functions as subscribers, handling async callbacks.
pub struct CallbackSubscriberDartFn {
    on_update: Box<dyn Fn(String) -> DartFnFuture<()> + Send + Sync>, // Async update callback
    on_error: Box<dyn Fn(String, Option<String>) -> DartFnFuture<()> + Send + Sync>, // Async error callback
}

impl QuerySubscriber for CallbackSubscriberDartFn {
    fn on_update(&self, value: String) {
        let future = (self.on_update)(value);
        tokio::spawn(async move {
            let _ = future.await; // Await the future, ignoring the result
        });
    }

    fn on_error(&self, message: String, value: Option<String>) {
        let future = (self.on_error)(message, value);
        tokio::spawn(async move {
            let _ = future.await;
        });
    }
}

/// Type alias for the auth error callback sender (sends errors to Dart)
type AuthErrorSender = mpsc::UnboundedSender<AuthError>;
/// Type alias for the auth error action receiver (receives actions from Dart)
type AuthActionReceiver = std::sync::mpsc::Receiver<AuthErrorAction>;
/// Type alias for the auth error action sender (Dart sends actions through this)
type AuthActionSender = std::sync::mpsc::Sender<AuthErrorAction>;

/// Stream receiver for auth errors. Dart can poll this to receive auth errors.
#[frb(opaque)]
pub struct AuthErrorStreamReceiver {
    receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<AuthError>>>,
}

impl AuthErrorStreamReceiver {
    /// Receives the next auth error from the stream.
    /// Returns None if the stream is closed.
    #[frb]
    pub async fn recv(&self) -> Option<AuthError> {
        self.receiver.lock().await.recv().await
    }
}

/// Main Convex client struct, opaque to Dart, managing connections and operations.
#[frb(opaque)]
pub struct MobileConvexClient {
    deployment_url: String,         // URL of the Convex deployment
    client_id: String,              // Client ID for authentication
    client: OnceCell<Arc<tokio::sync::Mutex<ConvexClient>>>, // Lazy-initialized Convex client
    rt: tokio::runtime::Runtime,    // Tokio runtime for async operations
    // Channel for sending auth errors to Dart
    auth_error_sender: Arc<Mutex<Option<AuthErrorSender>>>,
    // Channel for receiving auth error responses from Dart
    auth_action_receiver: Arc<Mutex<Option<AuthActionReceiver>>>,
    // Sender that Dart uses to respond to auth errors
    auth_action_sender: Arc<Mutex<Option<AuthActionSender>>>,
}

impl MobileConvexClient {
    /// Creates a new MobileConvexClient instance with the given deployment URL and client ID.
    #[frb(sync)]
    pub fn new(deployment_url: String, client_id: String) -> MobileConvexClient {
        let _ = ring::default_provider().install_default();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        MobileConvexClient {
            deployment_url,
            client_id,
            client: OnceCell::new(),
            rt,
            auth_error_sender: Arc::new(Mutex::new(None)),
            auth_action_receiver: Arc::new(Mutex::new(None)),
            auth_action_sender: Arc::new(Mutex::new(None)),
        }
    }

    /// Registers an auth error callback. Returns a stream receiver for auth errors.
    /// When an auth error occurs, it will be sent through this stream.
    /// Dart should call `respond_to_auth_error` with the appropriate action.
    #[frb]
    pub fn register_auth_error_handler(&self) -> AuthErrorStreamReceiver {
        // Create channels for bidirectional communication
        let (error_tx, error_rx) = mpsc::unbounded_channel::<AuthError>();
        let (action_tx, action_rx) = std::sync::mpsc::channel::<AuthErrorAction>();

        // Store the sender and receiver
        *self.auth_error_sender.lock() = Some(error_tx);
        *self.auth_action_receiver.lock() = Some(action_rx);
        *self.auth_action_sender.lock() = Some(action_tx);

        AuthErrorStreamReceiver { receiver: Arc::new(tokio::sync::Mutex::new(error_rx)) }
    }

    /// Responds to an auth error with the specified action.
    /// This should be called after receiving an auth error through the stream.
    #[frb(sync)]
    pub fn respond_to_auth_error(&self, action: AuthErrorAction) {
        if let Some(sender) = self.auth_action_sender.lock().as_ref() {
            let _ = sender.send(action);
        }
    }

    /// Retrieves or initializes a connected Convex client.
    fn connected_client(&self) -> anyhow::Result<Arc<tokio::sync::Mutex<ConvexClient>>> {
        // Use get_or_try_init to initialize once
        self.client
            .get_or_try_init(|| {
                let url = self.deployment_url.clone();
                let client_id = self.client_id.clone();
                let rt_handle = self.rt.handle().clone();
                let error_sender = self.auth_error_sender.clone();
                let action_receiver = self.auth_action_receiver.clone();

                // Use block_in_place to allow blocking from async context,
                // then run on our dedicated runtime
                tokio::task::block_in_place(|| {
                    rt_handle.block_on(async move {
                        // Create the auth error callback
                        let auth_callback: Arc<dyn Fn(&ConvexAuthError) -> ConvexAuthErrorAction + Send + Sync> =
                            Arc::new(move |error: &ConvexAuthError| {
                                // Try to send the error to Dart
                                if let Some(sender) = error_sender.lock().as_ref() {
                                    let dart_error = AuthError::from(error);
                                    let _ = sender.send(dart_error);

                                    // Wait for response from Dart with a timeout
                                    if let Some(receiver) = action_receiver.lock().as_ref() {
                                        match receiver.recv_timeout(Duration::from_secs(30)) {
                                            Ok(action) => return action.into(),
                                            Err(_) => {
                                                // Timeout or channel closed - default to ClearAuth
                                                return ConvexAuthErrorAction::ClearAuth;
                                            }
                                        }
                                    }
                                }
                                // No handler registered - default to ClearAuth
                                ConvexAuthErrorAction::ClearAuth
                            });

                        let client = ConvexClientBuilder::new(url.as_str())
                            .with_client_id(&client_id)
                            .with_on_auth_error(auth_callback)
                            .build()
                            .await?;

                        // Give the WebSocket a moment to establish connection
                        tokio::time::sleep(Duration::from_millis(1000)).await;

                        Ok::<_, anyhow::Error>(Arc::new(tokio::sync::Mutex::new(client)))
                    })
                })
            })
            .map(|client_ref| client_ref.clone())
    }

    /// Executes a query on the Convex backend.
    #[frb]
    pub async fn query(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let client = self.connected_client()?;
        let result = self.rt
            .spawn(async move {
                let mut client = client.lock().await;
                client.query(name.as_str(), parse_json_args(args)).await
            })
            .await
            .map_err(|e| anyhow::anyhow!("Join error: {:?}", e))??;
        handle_direct_function_result(result)
    }

    /// Subscribes to real-time updates from a Convex query.
    #[frb]
    pub async fn subscribe(
        &self,
        name: String,
        args: HashMap<String, String>,
        on_update: impl Fn(String) -> DartFnFuture<()> + Send + Sync + 'static,
        on_error: impl Fn(String, Option<String>) -> DartFnFuture<()> + Send + Sync + 'static,
    ) -> Result<SubscriptionHandle, ClientError> {
        let subscriber = Arc::new(CallbackSubscriberDartFn {
            on_update: Box::new(on_update),
            on_error: Box::new(on_error),
        });
        self.internal_subscribe(name, args, subscriber).await.map_err(Into::into)
    }

    /// Internal method for subscription logic.
    async fn internal_subscribe(
        &self,
        name: String,
        args: HashMap<String, String>,
        subscriber: Arc<dyn QuerySubscriber>,
    ) -> anyhow::Result<SubscriptionHandle> {
        let client = self.connected_client()?;

        let (cancel_sender, cancel_receiver) = oneshot::channel::<()>();
        let sub_name = name;
        let parsed_args = parse_json_args(args);

        // Spawn EVERYTHING on self.rt - subscription creation AND the loop
        self.rt.spawn(async move {
            // Create subscription while holding the lock, then release it
            let mut subscription = {
                let mut client_guard = client.lock().await;
                match client_guard.subscribe(&sub_name, parsed_args).await {
                    Ok(sub) => sub,
                    Err(e) => {
                        subscriber.on_error(format!("Failed to subscribe: {:?}", e), None);
                        return;
                    }
                }
            };
            // Lock is released here, subscription is independent

            let cancel_fut = cancel_receiver.fuse();
            pin_mut!(cancel_fut);

            loop {
                select_biased! {
                    new_val = subscription.next().fuse() => {
                        let new_val = new_val.expect("Client dropped prematurely");

                        match new_val {
                            FunctionResult::Value(value) => {
                                subscriber.on_update(serde_json::to_string(
                                    &serde_json::Value::from(value),
                                ).unwrap());
                            }
                            FunctionResult::ErrorMessage(message) => {
                                subscriber.on_error(message, None);
                            }
                            FunctionResult::ConvexError(error) => {
                                subscriber.on_error(
                                    error.message,
                                    Some(serde_json::ser::to_string(
                                        &serde_json::Value::from(error.data),
                                    ).unwrap()),
                                );
                            }
                        }
                    }
                    _ = cancel_fut => {
                        break;
                    }
                }
            }
        });

        Ok(SubscriptionHandle::new(cancel_sender))
    }

    /// Executes a mutation on the Convex backend.
    #[frb]
    pub async fn mutation(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let result = self.internal_mutation(name, args).await?;
        handle_direct_function_result(result)
    }

    /// Internal method for mutation logic.
    async fn internal_mutation(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> anyhow::Result<FunctionResult> {
        let client = self.connected_client()?;
        self.rt
            .spawn(async move {
                let mut client_guard = client.lock().await;
                client_guard.mutation(&name, parse_json_args(args)).await
            })
            .await
            .map_err(|e| anyhow::anyhow!("Join error: {:?}", e))?
    }

    /// Executes an action on the Convex backend.
    #[frb]
    pub async fn action(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let result = self.internal_action(name, args).await?;
        handle_direct_function_result(result)
    }

    /// Internal method for action logic.
    async fn internal_action(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> anyhow::Result<FunctionResult> {
        let client = self.connected_client()?;
        self.rt
            .spawn(async move {
                let mut client_guard = client.lock().await;
                client_guard.action(&name, parse_json_args(args)).await
            })
            .await
            .map_err(|e| anyhow::anyhow!("Join error: {:?}", e))?
    }

    /// Sets authentication token for the client.
    #[frb]
    pub async fn set_auth(&self, token: Option<String>) -> Result<(), ClientError> {
        Ok(self.internal_set_auth(token).await?)
    }

    /// Internal method for setting authentication.
    async fn internal_set_auth(&self, token: Option<String>) -> anyhow::Result<()> {
        let client = self.connected_client()?;
        self.rt
            .spawn(async move {
                let mut client_guard = client.lock().await;
                client_guard.set_auth(token).await
            })
            .await
            .map_err(|e| anyhow::anyhow!("Join error: {:?}", e))?;
        Ok(())
    }
}

/// Utility function to parse HashMap arguments into Convex Value format.
fn parse_json_args(raw_args: HashMap<String, String>) -> BTreeMap<String, Value> {
    raw_args
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                Value::try_from(
                    serde_json::from_str::<serde_json::Value>(&v)
                        .expect("Invalid JSON data from FFI"),
                )
                .expect("Invalid Convex data from FFI"),
            )
        })
        .collect()
}

/// Utility function to handle and serialize FunctionResult into a string or error.
fn handle_direct_function_result(result: FunctionResult) -> Result<String, ClientError> {
    match result {
        FunctionResult::Value(v) => serde_json::to_string(&serde_json::Value::from(v))
            .map_err(|e| ClientError::InternalError { msg: e.to_string() }),
        FunctionResult::ConvexError(e) => Err(ClientError::ConvexError {
            data: serde_json::ser::to_string(&serde_json::Value::from(e.data)).unwrap(),
        }),
        FunctionResult::ErrorMessage(msg) => Err(ClientError::ServerError { msg }),
    }
}
