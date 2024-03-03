use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::Datastore;
use crate::datastore::TxOffset;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::omnipaxos_durability::OmniLogEntry;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use tokio::{sync::mpsc, time};

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    // TODO Messaging and running
    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self
            .node
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omnipaxos
            .outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(msg).await;
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                biased;
                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().update_leader();
                },
                _ = outgoing_interval.tick() => { 
                    self.send_outgoing_msgs().await; 
                    // Call apply_replicated_txns
                    self.node.lock().unwrap().apply_replicated_txns();
                },
                Some(msg) = self.incoming.recv() => {
                    self.node.lock().unwrap().omni_paxos_durability.omnipaxos.handle_incoming(msg);
                }
            }
        }
    }
}

pub struct Node {
    pub node_id: NodeId,
    pub omni_paxos_durability: OmniPaxosDurability,
    pub datastore: ExampleDatastore,
    pub leader_id: Option<NodeId>,
    pub latest_decided_idx: u64,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        return Node{
            node_id: node_id,
            // TODO Datastore and OmniPaxosDurability
            omni_paxos_durability:omni_durability,
            datastore: ExampleDatastore::new(),
            leader_id: None,
            latest_decided_idx:0
        };
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        // TODO
        let leader_id = self.omni_paxos_durability.omnipaxos.get_current_leader();
        if leader_id.is_some() && leader_id.unwrap() != self.node_id {
            self.leader_id = leader_id;
        } else {
            self.leader_id = None;
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        // TODO
        let mut idx = self.latest_decided_idx;
        let iddex: u64 = self.omni_paxos_durability.omnipaxos.get_decided_idx();
        if idx < iddex {
            let entries =self.omni_paxos_durability.iter_starting_from_offset(TxOffset( idx));
            for (txOffset, txData) in entries {
                let mut tx = self.begin_mut_tx().unwrap();
                // Create a mutable vector to act as a buffer
                let mut buffer = Vec::new();

                // Convert the buffer into a Write type (Cursor)
                let mut cursor = Cursor::new(&mut buffer);
                txData.serialize(&mut cursor);
                let value = String::from_utf8(buffer).unwrap();      
                tx.set(txOffset.0.to_string(), value);    
                idx += 1;
            }
        }
        self.latest_decided_idx = idx;
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        // TODO
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        // TODO
        self.datastore.release_tx(tx)
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        // TODO
        Ok(self.datastore.begin_mut_tx())
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        // TODO
        self.datastore.commit_mut_tx(tx)
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        // TODO
        let result=self.datastore.get_replicated_offset();
        match result{
            Some(offset)=> self.datastore.advance_replicated_durability_offset(offset),
            None=> Err(DatastoreError::ReplicatedOffsetNotAvailable)
        }
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
        HashMap<NodeId, mpsc::Receiver<Message<OmniLogEntry>>>,
    ) {
        todo!()
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels();
        for pid in SERVERS {
            todo!("spawn the nodes")
        }
        nodes
    }
}