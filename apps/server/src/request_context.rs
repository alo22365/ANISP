use std::time::Instant;

use anisp_common::ErrorResponse;
use axum::{
    Json,
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::{Instrument, field, info, info_span};
use uuid::Uuid;

use crate::error::HttpError;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

pub(crate) async fn middleware(mut request: Request<Body>, next: Next) -> Response {
    let request_id = Uuid::now_v7();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = Instant::now();
    let span = info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        path = %path,
        status = field::Empty,
        duration_ms = field::Empty,
    );

    request.extensions_mut().insert(request_id);
    let mut response = next.run(request).instrument(span.clone()).await;
    if let Some(error) = response.extensions_mut().remove::<HttpError>() {
        response = (
            response.status(),
            Json(ErrorResponse::new(
                error.code,
                error.message,
                request_id.to_string(),
            )),
        )
            .into_response();
    }

    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id.to_string()).expect("UUID is a valid header value"),
    );
    span.record("status", response.status().as_u16());
    span.record("duration_ms", started.elapsed().as_millis() as u64);
    info!(parent: &span, "HTTP request completed");
    response
}
