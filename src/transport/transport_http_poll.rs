//! Poll transport — SSE-based message delivery.
//!
//! Workers connect via `GET /poll/{group}/{id}` and receive messages
//! as Server-Sent Events. The server holds connections open and pushes
//! messages to them based on poll:// address routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::{PollAddress, PollCast};

/// A single SSE connection to a worker.
pub struct PollConnection {
    /// Unique identifier for this specific connection instance.
    pub conn_id: u64,
    pub id: String,
    pub tx: mpsc::Sender<String>,
    /// Fires when a newer connection claims this same group and id.
    close_tx: StdMutex<Option<oneshot::Sender<()>>>,
}

impl PollConnection {
    fn request_close(&self) {
        if let Ok(mut slot) = self.close_tx.lock() {
            if let Some(tx) = slot.take() {
                let _ = tx.send(());
            }
        }
    }
}

/// Manages all active poll connections, grouped by group name.
pub struct PollRegistry {
    /// group -> [connection]
    connections: Mutex<HashMap<String, Vec<Arc<PollConnection>>>>,
    /// Monotonically increasing counter for unique connection IDs.
    next_conn_id: AtomicU64,
    pub max_connections: usize,
    pub buffer_size: usize,
}

impl PollRegistry {
    pub fn new(max_connections: usize, buffer_size: usize) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            max_connections,
            buffer_size,
        }
    }

    /// Register a connection. A newer connection with the same group and id
    /// replaces the older one and closes it. Returns None if max connections
    /// would be exceeded by a connection that is not a replacement.
    pub async fn register(
        &self,
        group: &str,
        id: &str,
    ) -> Option<(
        Arc<PollConnection>,
        mpsc::Receiver<String>,
        oneshot::Receiver<()>,
    )> {
        let mut conns = self.connections.lock().await;

        // The worker is the source of truth for its identity. A reconnect
        // presents the same group and id while the old TCP connection can
        // still be registered. Delivering to the first match would starve
        // the live connection, so the stale record is closed and removed
        // before the capacity check.
        let mut replaced_group_is_empty = false;
        if let Some(group_conns) = conns.get_mut(group) {
            let (stale, live): (Vec<_>, Vec<_>) =
                group_conns.drain(..).partition(|conn| conn.id == id);
            *group_conns = live;
            for old in stale {
                old.request_close();
            }
            replaced_group_is_empty = group_conns.is_empty();
        }
        if replaced_group_is_empty {
            conns.remove(group);
        }

        let total: usize = conns.values().map(|v| v.len()).sum();
        if total >= self.max_connections {
            return None;
        }

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.buffer_size);
        let (close_tx, close_rx) = oneshot::channel();
        let conn = Arc::new(PollConnection {
            conn_id,
            id: id.to_string(),
            tx,
            close_tx: StdMutex::new(Some(close_tx)),
        });

        conns
            .entry(group.to_string())
            .or_default()
            .push(conn.clone());

        tracing::info!(
            group = %group,
            id = %id,
            conn_id = conn_id,
            total_connections = total + 1,
            "Poll connection registered"
        );

        Some((conn, rx, close_rx))
    }

    /// Deregister a specific connection by its unique connection ID.
    pub async fn deregister(&self, group: &str, conn_id: u64) {
        let mut conns = self.connections.lock().await;
        if let Some(group_conns) = conns.get_mut(group) {
            group_conns.retain(|c| c.conn_id != conn_id);
            if group_conns.is_empty() {
                conns.remove(group);
            }
        }
        tracing::info!(group = %group, conn_id = conn_id, "Poll connection deregistered");
    }

    /// Send a message to the appropriate connection(s) based on the poll address.
    /// Returns true if the message was delivered, false otherwise.
    pub async fn send_poll(&self, address: &PollAddress, payload: &str) -> bool {
        let conns = self.connections.lock().await;

        let group_conns = match conns.get(&address.group) {
            Some(c) if !c.is_empty() => c,
            _ => {
                tracing::warn!(
                    group = %address.group,
                    "Poll send failed: no active connections for group"
                );
                return false;
            }
        };

        let delivered = match address.cast {
            PollCast::Uni => {
                // Must have an id, must match exactly
                if let Some(target_id) = &address.id {
                    if let Some(conn) = group_conns.iter().find(|c| &c.id == target_id) {
                        conn.tx.try_send(payload.to_string()).is_ok()
                    } else {
                        tracing::warn!(
                            group = %address.group,
                            target_id = %target_id,
                            "Poll send failed: target connection not found in group"
                        );
                        false
                    }
                } else {
                    false
                }
            }
            PollCast::Any => {
                // Prefer specific id, fall back to random
                if let Some(target_id) = &address.id {
                    if let Some(conn) = group_conns.iter().find(|c| &c.id == target_id) {
                        let ok = conn.tx.try_send(payload.to_string()).is_ok();
                        if ok {
                            tracing::debug!(
                                group = %address.group,
                                target_id = %target_id,
                                "Poll message delivered to preferred connection"
                            );
                        }
                        return ok;
                    }
                }
                // Fall back to random selection to distribute work
                let idx = fastrand::usize(..group_conns.len());
                let ok = group_conns[idx].tx.try_send(payload.to_string()).is_ok();
                if ok {
                    tracing::debug!(
                        group = %address.group,
                        selected_id = %group_conns[idx].id,
                        "Poll message delivered to random connection"
                    );
                }
                ok
            }
        };
        if !delivered {
            tracing::warn!(
                group = %address.group,
                "Poll message delivery failed: channel full or closed"
            );
        }
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::super::{PollAddress, PollCast};
    use super::PollRegistry;

    fn uni(group: &str, id: &str) -> PollAddress {
        PollAddress {
            cast: PollCast::Uni,
            group: group.to_string(),
            id: Some(id.to_string()),
        }
    }

    #[tokio::test]
    async fn duplicate_identity_delivers_to_the_newest_connection() {
        let registry = PollRegistry::new(8, 8);
        let (other, mut other_rx, _other_close) = registry.register("g", "other").await.unwrap();
        let (old, mut old_rx, mut old_close) = registry.register("g", "pid").await.unwrap();
        let (new, mut new_rx, _new_close) = registry.register("g", "pid").await.unwrap();

        assert_ne!(old.conn_id, new.conn_id);
        old_close
            .try_recv()
            .expect("the stale connection should be closed");
        assert!(registry.send_poll(&uni("g", "pid"), "hello").await);
        assert!(registry.send_poll(&uni("g", "other"), "peer").await);
        assert_eq!(other_rx.try_recv().unwrap(), "peer");
        let _ = other;
        assert_eq!(new_rx.try_recv().unwrap(), "hello");
        assert!(old_rx.try_recv().is_err());

        registry.deregister("g", old.conn_id).await;
        assert!(registry.send_poll(&uni("g", "pid"), "kept").await);
        assert_eq!(new_rx.try_recv().unwrap(), "kept");

        registry.deregister("g", new.conn_id).await;
        assert!(!registry.send_poll(&uni("g", "pid"), "gone").await);
    }

    #[tokio::test]
    async fn replacement_does_not_consume_a_second_connection_slot() {
        let registry = PollRegistry::new(1, 8);
        let _old = registry.register("g", "pid").await.unwrap();
        assert!(
            registry.register("g", "pid").await.is_some(),
            "replacing the only connection must not be rejected as over capacity"
        );
    }
}
