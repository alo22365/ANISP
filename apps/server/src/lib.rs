mod error;
mod handler;
mod request_context;
mod routes;
mod state;

pub use error::{AppError, AppResult};
pub use state::AppState;

use axum::{Router, middleware};

/// Builds the complete HTTP application.
///
/// Keeping router construction in the library makes the transport layer easy to
/// test without binding a TCP port.
pub fn build_app(state: AppState) -> Router {
    routes::router()
        .with_state(state)
        .layer(middleware::from_fn(request_context::middleware))
}
