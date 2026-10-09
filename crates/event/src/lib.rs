//! Context event domain and application service.

pub mod dto;
pub mod error;
pub mod model;
pub mod query;
pub mod repository;
pub mod service;

pub use dto::{
    CreateEventRequest, CreateEventResponse, EventResponse, PreparedContextEvent,
    SearchEventsRequest, SearchEventsResponse,
};
pub use error::{EventServiceError, RepositoryError, ValidationError};
pub use model::{ContextEvent, StoredContextEvent};
pub use query::{EventFilters, EventQuery};
pub use repository::EventRepository;
pub use service::{EventService, MAX_EVENT_METADATA_BYTES};
