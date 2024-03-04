use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::example_datastore::MutTx;
use crate::datastore::tx_data::TxData;
use crate::datastore::tx_data::TxResult;
use crate::datastore::Datastore;
use crate::datastore::TxOffset;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::omnipaxos_durability::OmniLogEntry;
use crate::durability::DurabilityLevel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use omnipaxos::messages::Message;
use omnipaxos::util::LogEntry;
use omnipaxos::util::NodeId;
use omnipaxos::OmniPaxos;
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::{sync::mpsc, time};

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);


type OmniPaxosKV = OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>;

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
                    self.node.lock().unwrap().omni_paxos_durability.omnipaxos.tick();
                    self.node.lock().unwrap().update_leader();
                },
                _ = outgoing_interval.tick() => { 
                    self.send_outgoing_msgs().await; 
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
    pub latest_decided_idx: u64,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        return Node{
            node_id: node_id,
            // TODO Datastore and OmniPaxosDurability
            omni_paxos_durability:omni_durability,
            datastore: ExampleDatastore::new(),
            latest_decided_idx:0
        };
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        let leader_id = match self.omni_paxos_durability.omnipaxos.get_current_leader(){
            Some(id)=> id,
            None=>return {}
        };
        if leader_id == self.node_id {
            self.apply_replicated_txns();
        } else {
            self.rollback_unreplicated_txns();
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let current_idx: u64 = self.omni_paxos_durability.omnipaxos.get_decided_idx();
        if current_idx > self.latest_decided_idx {
            let decided_entries= self.omni_paxos_durability.omnipaxos.read_decided_suffix(self.latest_decided_idx).unwrap();
            self.update_database(decided_entries);
            self.latest_decided_idx = current_idx;
            self.advance_replicated_durability_offset().unwrap();
        }
    }

    fn log_entry_to_db_entry(&self, tx_offset:&TxOffset, tx_data:&TxData)->MutTx{
        let mut tx = self.datastore.begin_mut_tx();
        let tx_offset_str = tx_offset.0.to_string();
            
        let mut bytes = Vec::new();
        let _ = tx_data.serialize(&mut bytes).unwrap();
        let tx_data_str  = String::from_utf8(bytes).expect("Invalid UTF-8 sequence");

        tx.set(tx_offset_str, tx_data_str);
        tx
    }

    fn update_database(&self, decided_entries: Vec<LogEntry<OmniLogEntry>>) {
        for entry in decided_entries {
            match entry {
                LogEntry::Decided(entry) => {
                    let tx = self.log_entry_to_db_entry(&entry.tx_offset, &entry.tx_data);
                    self.datastore.commit_mut_tx(tx).unwrap_or_else(|err| panic!("Error in update database commit {:?}", err));
                }
                _ => {}
            }
        }
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.datastore.release_tx(tx)
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        Ok(self.datastore.begin_mut_tx())
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        self.datastore.commit_mut_tx(tx)
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
     let result = self.datastore.get_replicated_offset();
       match result {
           Some(offset) => self.datastore.advance_replicated_durability_offset(offset),
           None => Err(DatastoreError::ReplicatedOffsetNotAvailable),
       }
    }
    
    fn rollback_unreplicated_txns(&mut self) {
        let current_idx: u64 = self.omni_paxos_durability.omnipaxos.get_decided_idx();
        let committed_idx = self.latest_decided_idx;
        if current_idx < committed_idx {
           _ = self.datastore.rollback_to_replicated_durability_offset();
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
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

    fn get_example_transaction(datastore: &ExampleDatastore)->MutTx{
        let mut tx1 = datastore.begin_mut_tx();
        tx1.set("foo".to_string(), "bar".to_string());
        tx1

    }

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
        HashMap<NodeId, mpsc::Receiver<Message<OmniLogEntry>>>,
    ) {
        // TODO: Implement channel initialization
        let mut sender_channels = HashMap::new();
        let mut receiver_channels = HashMap::new();
    
        for pid in SERVERS {
            let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
            sender_channels.insert(pid, sender);
            receiver_channels.insert(pid, receiver);
        }
        (sender_channels, receiver_channels)
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
        let mut nodes = HashMap::new();
        for pid in SERVERS {
            // TODO: Spawn the nodes
            let server_config = ServerConfig {
                pid,
                election_tick_timeout: ELECTION_TICK_TIMEOUT,
                ..Default::default()
            };
            let configuration_id = 1;
            let cluster_config = ClusterConfig {
                configuration_id,
                nodes: SERVERS.into(),
                ..Default::default()
            };
            let omnipaxos_config = OmniPaxosConfig {
                server_config,
                cluster_config,
            };
            let storage: MemoryStorage<OmniLogEntry> = MemoryStorage::default();
            let omnipaxos = omnipaxos_config.build( storage).unwrap();
            let omnipaxos_durability = OmniPaxosDurability::new(omnipaxos);
            let  node = Arc::new(Mutex::new(Node::new(pid, omnipaxos_durability)));
            let mut node_runner: NodeRunner = NodeRunner {
                node: node.clone(),
                incoming: receiver_channels.remove(&pid).unwrap(),
                outgoing: sender_channels.clone(),
            };
            let handle = runtime.spawn(async move {
                node_runner.run().await; 
            });
            
            nodes.insert(pid,(node, handle));
        }
        nodes
    }

    #[tokio::test]
    async fn test_find_leader_and_commit_transaction() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        let node1 = nodes.get(&1).unwrap();
        let node1_obj = node1.0.lock().unwrap();
        // TODO: Implement the test case
        let leader_id = node1_obj.omni_paxos_durability.omnipaxos.get_current_leader().unwrap();
        
        

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_kill_leader_and_elect_new_leader() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);

        // TODO: Implement the test case

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_disconnect_leader_and_rollback_transactions() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);

        // TODO: Implement the test case

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_partial_connectivity_scenarios() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);

        // TODO: Implement the test case

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_commit_transactions_1() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        // wait for leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let (first_server, _) = nodes.get(&1).unwrap();

        // check which server is the current leader

        let leader = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability.omnipaxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Elected leader: {}", leader);

        let (leader_server, _leader_join_handle) = nodes.get(&leader).unwrap();
        //add a mutable transaction to the leader
        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("foo".to_string(), "bar".to_string());
        let _result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();

        // wait for the entries to be decided...
        println!("Trasaction committed");

        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 2);
        let last_replicated_tx = leader_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        // check that the transaction was replicated in leader
        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            Some("bar".to_string())
        );
        leader_server.lock().unwrap().release_tx(last_replicated_tx);
        // check that the transaction was replicated in the followers
        let follower = (leader + 1) as u64 % nodes.len() as u64;
        let (follower_server, _) = nodes.get(&follower).unwrap();
        let last_replicated_tx = follower_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            Some("bar".to_string())
        );

        follower_server
            .lock()
            .unwrap()
            .release_tx(last_replicated_tx);

        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("red".to_string(), "blue".to_string());
        let _result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();

        // wait for the entries to be decided...
        println!("Trasaction committed");
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 4);
        let last_replicated_tx = leader_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        // check that the transaction was replicated in leader
        assert_eq!(
            last_replicated_tx.get(&"red".to_string()),
            Some("blue".to_string())
        );
        leader_server.lock().unwrap().release_tx(last_replicated_tx);

        let last_replicated_tx = follower_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        // check that the transaction was replicated in leader
        assert_eq!(
            last_replicated_tx.get(&"red".to_string()),
            Some("blue".to_string())
        );
        leader_server.lock().unwrap().release_tx(last_replicated_tx);
    }
}