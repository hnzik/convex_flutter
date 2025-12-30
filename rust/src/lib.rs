mod frb_generated;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use android_logger::Config;
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
use log::{debug, LevelFilter};
use parking_lot::Mutex;
use rustls::crypto::ring;
#[cfg(target_os = "android")]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
}

impl MobileConvexClient {
    /// Creates a new MobileConvexClient instance with the given deployment URL and client ID.
    #[frb(sync)]
    pub fn new(deployment_url: String, client_id: String) -> MobileConvexClient {
        // Initialize logger for Android (works in both debug and release)
        android_logger::init_once(Config::default().with_max_level(LevelFilter::Debug));

        let _ = ring::default_provider().install_default();

        // Initialize tracing for convex crate debug output
        #[cfg(target_os = "android")]
        {
            // On Android, output to logcat via tracing-android
            let _ = tracing_subscriber::registry()
                .with(tracing_android::layer("convex_flutter").unwrap())
                .with(tracing_subscriber::filter::LevelFilter::DEBUG)
                .try_init();
        }

        #[cfg(not(target_os = "android"))]
        {
            // On other platforms, use default subscriber
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
                .try_init();
        }

        log::error!("[CONVEX] Tracing initialized");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        MobileConvexClient {
            deployment_url,
            client_id,
            client: OnceCell::new(),
            rt,
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
                        log::error!("[CONVEX] Building ConvexClient...");
                        let client = ConvexClientBuilder::new(url.as_str())
                            .with_client_id(&client_id)
                            .build()
                            .await?;
                        log::error!("[CONVEX] ConvexClient built");

                        // Give the WebSocket a moment to establish connection
                        log::error!("[CONVEX] Waiting for WebSocket to establish...");
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        log::error!("[CONVEX] Client ready");

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
        log::error!("[CONVEX] query() called for: {}", name);
        let client = self.connected_client()?;
        log::error!("[CONVEX] query() got client for: {}", name);
        let name_clone = name.clone();
        let result = self.rt
            .spawn(async move {
                let mut client = client.lock().await;
                client.query(name_clone.as_str(), parse_json_args(args)).await
            })
            .await
            .map_err(|e| anyhow::anyhow!("Join error: {:?}", e))??;
        log::error!("[CONVEX] query() got result for: {}", name);
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
        log::error!("[CONVEX] subscribe() called for: {}", name);
        let subscriber = Arc::new(CallbackSubscriberDartFn {
            on_update: Box::new(on_update),
            on_error: Box::new(on_error),
        });
        let result = self.internal_subscribe(name.clone(), args, subscriber).await;
        log::error!("[CONVEX] subscribe() completed for: {}, success: {}", name, result.is_ok());
        result.map_err(Into::into)
    }

    /// Internal method for subscription logic.
    async fn internal_subscribe(
        &self,
        name: String,
        args: HashMap<String, String>,
        subscriber: Arc<dyn QuerySubscriber>,
    ) -> anyhow::Result<SubscriptionHandle> {
        log::error!("[CONVEX] internal_subscribe - getting client for: {}", name);
        let client = self.connected_client()?;
        log::error!("[CONVEX] internal_subscribe - got client for: {}", name);

        let (cancel_sender, cancel_receiver) = oneshot::channel::<()>();
        let sub_name = name.clone();
        let parsed_args = parse_json_args(args);

        // Spawn EVERYTHING on self.rt - subscription creation AND the loop
        self.rt.spawn(async move {
            log::error!("[CONVEX] Inside rt.spawn - creating subscription for: {}", sub_name);

            // Create subscription while holding the lock, then release it
            let mut subscription = {
                let mut client_guard = client.lock().await;
                match client_guard.subscribe(&sub_name, parsed_args).await {
                    Ok(sub) => {
                        log::error!("[CONVEX] Subscription created successfully for: {}", sub_name);
                        sub
                    }
                    Err(e) => {
                        log::error!("[CONVEX] Failed to create subscription for {}: {:?}", sub_name, e);
                        subscriber.on_error(format!("Failed to subscribe: {:?}", e), None);
                        return;
                    }
                }
            };
            // Lock is released here, subscription is independent

            log::error!("[CONVEX] Subscription loop started for: {}", sub_name);
            let cancel_fut = cancel_receiver.fuse();
            pin_mut!(cancel_fut);
            loop {
                log::error!("[CONVEX] Waiting for next value for: {}", sub_name);
                select_biased! {
                    new_val = subscription.next().fuse() => {
                        log::error!("[CONVEX] Got value from subscription: {}", sub_name);
                        let new_val = new_val.expect("Client dropped prematurely");
                        match new_val {
                            FunctionResult::Value(value) => {
                                log::error!("[CONVEX] Value received for: {}", sub_name);
                                subscriber.on_update(serde_json::to_string(
                                    &serde_json::Value::from(value),
                                ).unwrap());
                            }
                            FunctionResult::ErrorMessage(message) => {
                                log::error!("[CONVEX] ErrorMessage for {}: {}", sub_name, message);
                                subscriber.on_error(message, None);
                            }
                            FunctionResult::ConvexError(error) => {
                                log::error!("[CONVEX] ConvexError for {}: {}", sub_name, error.message);
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
                        log::error!("[CONVEX] Subscription cancelled: {}", sub_name);
                        break;
                    }
                }
            }
            log::error!("[CONVEX] Subscription loop ended: {}", sub_name);
        });

        log::error!("[CONVEX] internal_subscribe - returning handle for: {}", name);
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
        debug!("Running action: {}", name);
        let result = self.internal_action(name, args).await?;
        debug!("Got action result: {:?}", result);
        handle_direct_function_result(result)
    }

    /// Internal method for action logic.
    async fn internal_action(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> anyhow::Result<FunctionResult> {
        let client = self.connected_client()?;
        debug!("Running action: {}", name);
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
