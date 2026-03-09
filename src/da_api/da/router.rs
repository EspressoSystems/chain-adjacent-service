// src/da/router.rs

use crate::{
    da_api::da::client::DaClient,
    da_api::error::{DaApiError, DaApiResult},
};
use std::{collections::HashMap, sync::Arc};

/// Routes DA operations to the correct downstream client based on the DA type byte.
pub struct DaRouter {
    clients: HashMap<u8, Arc<dyn DaClient>>,
}

impl DaRouter {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    /// Register a downstream DA client.
    pub fn register(&mut self, client: Arc<dyn DaClient>) {
        self.clients.insert(client.da_type_byte(), client);
    }

    /// Look up a client by DA type byte.
    pub fn get(&self, da_type: u8) -> DaApiResult<Arc<dyn DaClient>> {
        self.clients
            .get(&da_type)
            .cloned()
            .ok_or_else(|| DaApiError::UnsupportedDaType(da_type))
    }

    /// All registered DA type bytes.
    pub fn registered_types(&self) -> Vec<u8> {
        self.clients.keys().copied().collect()
    }
}
