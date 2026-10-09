use chrono::{DateTime, Utc};

use crate::{K8sContextError, error::require_non_empty};

/// Current container facts, without stdout/stderr or log content. Source
/// times stay optional; Unknown means unavailable state, not Waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerState {
    Waiting {
        reason: Option<String>,
    },
    Running {
        started_at: Option<DateTime<Utc>>,
    },
    Terminated {
        exit_code: i32,
        reason: Option<String>,
        finished_at: Option<DateTime<Utc>>,
    },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerStatus {
    name: String,
    image: String,
    image_id: Option<String>,
    ready: bool,
    restart_count: u32,
    state: ContainerState,
}

impl ContainerStatus {
    pub fn new(
        name: impl Into<String>,
        image: impl Into<String>,
        image_id: Option<String>,
        ready: bool,
        restart_count: u32,
        state: ContainerState,
    ) -> Result<Self, K8sContextError> {
        let name = name.into();
        let image = image.into();
        require_non_empty(&name, K8sContextError::EmptyContainerName)?;
        require_non_empty(&image, K8sContextError::EmptyImage)?;
        Ok(Self {
            name,
            image,
            image_id,
            ready,
            restart_count,
            state,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn image(&self) -> &str {
        &self.image
    }
    pub fn image_id(&self) -> Option<&str> {
        self.image_id.as_deref()
    }
    pub fn ready(&self) -> bool {
        self.ready
    }
    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }
    pub fn state(&self) -> &ContainerState {
        &self.state
    }
}
