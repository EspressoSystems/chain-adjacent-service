use crate::da_api::da::client::DaClient;
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

    /// All registered DA type bytes.
    pub fn registered_types(&self) -> Vec<u8> {
        self.clients.keys().copied().collect()
    }
}
