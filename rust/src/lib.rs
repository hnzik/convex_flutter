mod frb_generated;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use once_cell::sync::OnceCell;
use convex::{
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

/// Main Convex client struct, opaque to Dart, managing connections and operations.
#[frb(opaque)]
pub struct MobileConvexClient {
    deployment_url: String,         // URL of the Convex deployment
    client_id: String,              // Client ID for authentication
    client: OnceCell<Arc<tokio::sync::Mutex<ConvexClient>>>, // Lazy-initialized Convex client
    rt: tokio::runtime::Runtime,    // Tokio runtime for async operations
    auth_expires_at: Arc<Mutex<Option<u64>>>, // Auth token expiration timestamp (Unix seconds)
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
            auth_expires_at: Arc::new(Mutex::new(None)),
        }
    }

    /// Parses a JWT token and extracts the expiration timestamp.
    fn parse_jwt_expiration(token: &str) -> Option<u64> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        // Decode the payload (second part)
        let payload = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
        let payload_str = String::from_utf8(payload).ok()?;
        let json: serde_json::Value = serde_json::from_str(&payload_str).ok()?;

        // Extract exp claim
        json.get("exp")?.as_u64()
    }

    /// Checks if the current auth token is still valid.
    /// Returns Ok(()) if valid or no token is set, Err if expired.
    /// If expired, also clears the auth token from the underlying client to stop retry loops.
    fn assert_auth_valid(&self) -> Result<(), ClientError> {
        let expires_at = *self.auth_expires_at.lock();
        if let Some(exp) = expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if now >= exp {
                // Clear our tracked expiration
                *self.auth_expires_at.lock() = None;
                // Clear the auth token in the underlying client to stop the retry loop
                self.clear_expired_auth();
                return Err(ClientError::InternalError {
                    msg: format!("Auth token expired at {}", exp),
                });
            }
        }
        Ok(())
    }

    /// Clears the auth token from the underlying client.
    /// Called when token expiration is detected to stop the reconnect retry loop.
    fn clear_expired_auth(&self) {
        if let Some(client) = self.client.get() {
            let client = client.clone();
            self.rt.spawn(async move {
                let mut client_guard = client.lock().await;
                client_guard.set_auth(None).await;
            });
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

                // Use block_in_place to allow blocking from async context,
                // then run on our dedicated runtime
                tokio::task::block_in_place(|| {
                    rt_handle.block_on(async move {
                        let client = ConvexClientBuilder::new(url.as_str())
                            .with_client_id(&client_id)
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
        self.assert_auth_valid()?;
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
        self.assert_auth_valid()?;
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
        let auth_expires_at = self.auth_expires_at.clone();
        let client_for_auth_clear = client.clone();

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

            // Helper closure to check auth and clear if expired
            let check_auth_expired = || -> bool {
                if let Some(exp) = *auth_expires_at.lock() {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    if now >= exp {
                        // Clear the expiration tracker
                        *auth_expires_at.lock() = None;
                        return true;
                    }
                }
                false
            };

            loop {
                // Check if auth token has expired before waiting for next value
                if check_auth_expired() {
                    // Clear auth from underlying client to stop retry loop
                    {
                        let mut client_guard = client_for_auth_clear.lock().await;
                        client_guard.set_auth(None).await;
                    }
                    subscriber.on_error(
                        "AUTH_EXPIRED".to_string(),
                        None,
                    );
                    break;
                }

                select_biased! {
                    new_val = subscription.next().fuse() => {
                        let new_val = new_val.expect("Client dropped prematurely");

                        // Check auth validity again after receiving a value
                        if check_auth_expired() {
                            // Clear auth from underlying client to stop retry loop
                            {
                                let mut client_guard = client_for_auth_clear.lock().await;
                                client_guard.set_auth(None).await;
                            }
                            subscriber.on_error(
                                "AUTH_EXPIRED".to_string(),
                                None,
                            );
                            break;
                        }

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
        self.assert_auth_valid()?;
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
        self.assert_auth_valid()?;
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
        // Parse and store the expiration time
        let expires_at = token.as_ref().and_then(|t| Self::parse_jwt_expiration(t));
        *self.auth_expires_at.lock() = expires_at;

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
