use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxData;
use crate::datastore::tx_data::TxResult;
use crate::datastore::Datastore;
use crate::datastore::TxOffset;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::omnipaxos_durability::OmniLogEntry;
use crate::durability::DurabilityLayer;
use crate::durability::DurabilityLevel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use omnipaxos::messages::Message;
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
pub const UPDATE_DB_PERIOD: Duration = Duration::from_millis(10);

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    // TODO Messaging and running
    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
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
            if !self.node.lock().unwrap().disconnected_nodes.contains(&receiver){
                let _ = channel.send(msg).await;
            }
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        let mut update_db_tick_interval = time::interval(UPDATE_DB_PERIOD );
        let mut remove_disconnected_nodes_interval = time::interval(UPDATE_DB_PERIOD );

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
                _ = update_db_tick_interval.tick() => {
                    self.node.lock().unwrap().apply_replicated_txns();
                },
                Some(msg) = self.incoming.recv() => {
                    let receiver = msg.get_receiver();
                    // if !self.node.lock().unwrap().disconnected_nodes.contains(&receiver){
                    self.node.lock().unwrap().omni_paxos_durability.omnipaxos.handle_incoming(msg);
                    // }
                
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
    pub latest_leader: u64,
    pub disconnected_nodes: Vec<NodeId>
}

impl Node {
    pub fn disconnect(&mut self, node_id: NodeId){
        self.disconnected_nodes.push(node_id);
    }

    pub fn reconnect(&mut self, node_id: NodeId){
        self.disconnected_nodes = self.disconnected_nodes.iter().filter_map(|&id| if id != node_id{ Some(id)} else {None} ).collect();
        
    }

    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        return Node{
            node_id: node_id,
            // TODO Datastore and OmniPaxosDurability
            omni_paxos_durability:omni_durability,
            datastore: ExampleDatastore::new(),
            latest_decided_idx:0,
            latest_leader:0,
            disconnected_nodes:vec![]
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
        if leader_id == self.node_id && self.latest_leader != self.node_id {
            self.apply_replicated_txns();
        } else if self.latest_leader == self.node_id && leader_id != self.node_id {
            self.rollback_unreplicated_txns();
        }
        self.latest_leader= leader_id;
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let current_idx: u64 = self.omni_paxos_durability.omnipaxos.get_decided_idx();
        if current_idx > self.latest_decided_idx {
            println!("node_id {:?} applies replicated txns", self.node_id);
            let decided_entries= self.omni_paxos_durability.iter_starting_from_offset(TxOffset(self.latest_decided_idx));
            
            self.update_database(decided_entries);
            //if self.latest_leader != self.node_id {
                self.latest_decided_idx = current_idx;

   //         }
            self.advance_replicated_durability_offset();
        }
    }

    fn update_database(&self, decided_entries:Box<dyn Iterator<Item = (TxOffset, TxData)>>) {
        for (tx_offset, tx_data) in decided_entries.into_iter() {
            //let tx = self.log_entry_to_db_entry(&tx_offset, &tx_data);
            for insert_list in tx_data.inserts.iter() {
                for insert in insert_list.inserts.iter() {
                    let mut tx = self.datastore.begin_mut_tx();
                    let row = example_datastore::deserialize_example_row(&insert.0).unwrap();
                    println!("Data appended from omnipaxos to datastore: {:?}, {:?}", row.key, row.value);
                    tx.set(row.key, row.value);
                    self.datastore.commit_mut_tx(tx).unwrap_or_else(|err| panic!("Error in update database commit {:?}", err));
                }
            }

            for delete_list in tx_data.deletes.iter() {
                for deleted_item in delete_list.deletes.iter() {
                    let mut tx = self.datastore.begin_mut_tx();
                    let row = example_datastore::deserialize_example_row(&deleted_item.0).unwrap();
                    println!("Data deleted from datastore: {:?}, {:?}", row.key, row.value);
                    tx.delete(&row.key);
                    self.datastore.commit_mut_tx(tx).unwrap_or_else(|err| panic!("Error in update database commit {:?}", err));
                }
            }
            let current_offset = self.datastore.get_cur_offset();
            let repl_offset = self.datastore.get_replicated_offset();
            println!("datastore current offset {:?}", current_offset);
            println!("datastore repl offset {:?}", repl_offset);
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
        // self.latest_decided_idx +=1 ;
        self.datastore.commit_mut_tx(tx)
    }

    fn advance_replicated_durability_offset(
        &self,
    ){
     let result: TxOffset = TxOffset(self.latest_decided_idx);
         
        let   _ =  self.datastore.advance_replicated_durability_offset(result);

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
    use crate::durability::DurabilityLayer;
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

    fn get_example_transaction(datastore: &ExampleDatastore)->example_datastore::MutTx{
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

        // wait for leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let (first_server, _) = nodes.get(&1).unwrap();

        // check which server is the current leader

        let leader_id = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability.omnipaxos
            .get_current_leader()
            .expect("Failed to get leader");

        let (leader_server, _leader_join_handle) = nodes.get(&leader_id).unwrap();
        //add a mutable transaction to the leader
        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("foo".to_string(), "bar".to_string());
        let result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

        // wait for the entries to be decided...
        println!("Trasaction committed");
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 6);
        let last_replicated_tx = leader_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        
        // check that the transaction was replicated in leader
        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            Some("bar".to_string())
        );
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_kill_leader_and_elect_new_leader() {
        // 2. Find the leader and commit a transaction. 
        //Kill the leader and show that another node will 
        //be elected and that the replicated state is still correct.

        let mut runtime = create_runtime();
        let mut nodes = spawn_nodes(&mut runtime);


        // wait for leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader_id = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability.omnipaxos
            .get_current_leader()
            .expect("Failed to get leader");

        let (leader_server, leader_join_handle) = nodes.get(&leader_id).unwrap();

        //add a mutable transaction to the leader
        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("foo".to_string(), "bar".to_string());
        let result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
       
        leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 6);
        let last_replicated_tx = leader_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        
        // check that the transaction was replicated in leader
        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            Some("bar".to_string())
        );

        // kill leader
        leader_join_handle.abort();
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 10);

        nodes.remove(&leader_id);
        let alive_servers:Vec<&u64> = SERVERS.iter().filter(|&&id| id != leader_id).collect();
        println!("leader killed {:?}", leader_id);
        println!("alive servers: {:?}", alive_servers);
        // get an alive node
        let (alive_server, handler) = nodes.get(alive_servers[0]).unwrap();

        let new_leader_id = alive_server.lock().unwrap().omni_paxos_durability.omnipaxos.get_current_leader().unwrap();
        println!("new leader id {:?}", new_leader_id);
        assert_ne!(new_leader_id, leader_id);
        
        let (new_leader,_) = nodes.get(&new_leader_id).unwrap();
        
        // check that the last replicated tx of the new leader is correct
        let last_replicated_tx = new_leader
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);

        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            Some("bar".to_string())
        );

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_disconnect_leader_and_rollback_transactions() {
        // initialize
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);

        // get leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability.omnipaxos
            .get_current_leader()
            .expect("Failed to get leader");
        
        // leader adds transaction
        let (leader_server, _leader_join_handle) = nodes.get(&leader).unwrap();
        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("foo".to_string(), "bar".to_string());

        // leader commits entry but doesn't append in omnipaxos
        let _result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader_server.lock().unwrap().latest_decided_idx += 1; 
        // leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);
        
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 2);

        // kill leader
        let _disc:Vec<()> = nodes.iter().map(|(node_id, _)| leader_server.lock().unwrap().disconnect(*node_id)).collect();

        std::thread::sleep(Duration::from_secs(2));
        
        let alive_servers:Vec<&u64> = SERVERS.iter().filter(|&&id| id != leader).collect();
        let (alive_server, handler) = nodes.get(alive_servers[0]).unwrap();

        let new_leader_id = alive_server.lock().unwrap().omni_paxos_durability.omnipaxos.get_current_leader().unwrap();
        println!("first leader {:?}", leader);
        println!("new leader id {:?}", new_leader_id);
        assert_ne!(new_leader_id, leader);
        
        let (new_leader,_) = nodes.get(&new_leader_id).unwrap();
        let _:Vec<()> = nodes.iter().map(|(node_id, _)| leader_server.lock().unwrap().reconnect(*node_id)).collect();

        std::thread::sleep(Duration::from_secs(2));

        // check that the last replicated tx of the new leader is correct
        let last_replicated_tx = leader_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);

        let last_commited_tx = leader_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Memory);

        println!("last in memory {:?}", last_commited_tx.get(&"foo".to_string()));
        println!( "Last rep: {:?}", last_replicated_tx.get(&"foo".to_string()));
        assert_eq!(
            last_replicated_tx.get(&"foo".to_string()),
            None
        ); 
        assert_eq!(
            last_commited_tx.get(&"foo".to_string()),
            None
        ); 
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_partial_connectivity_scenarios() {
        let mut runtime = create_runtime();
        let nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = spawn_nodes(&mut runtime);

        // wait for leader to be elected...
        std::thread::sleep(Duration::from_secs(1));
        let (first_server, _) = nodes.get(&1).unwrap();

        // ---------------------- chained scenario-----------------

        let leader_id = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability.omnipaxos
            .get_current_leader()
            .expect("Failed to get leader");

        let (leader_server,_ )= nodes.get(&leader_id).unwrap();
        println!("leader_server: {:?}", leader_id);

        let following_servers:Vec<&u64> = SERVERS.iter().filter(|&&id| id != leader_id).collect();

        let follower_id: u64 = *following_servers[0];
        let (follower_server, _) = nodes.get(&follower_id).unwrap();
        // follower_server.lock().unwrap().disconnect(leader_id);
        leader_server.lock().unwrap().disconnect(follower_id);
        std::thread::sleep(Duration::from_secs(3));
        
        let new_leader_id = follower_server
        .lock()
        .unwrap()
        .omni_paxos_durability.omnipaxos
        .get_current_leader()
        .expect("Failed to get leader"); 

        println!("new leader_server: {:?}", new_leader_id);

        assert_eq!(new_leader_id,follower_id);

        std::thread::sleep(Duration::from_secs(1));

        
        let new_leader_id = follower_server
        .lock()
        .unwrap()
        .omni_paxos_durability.omnipaxos
        .get_current_leader()
        .expect("Failed to get leader"); 

        assert_eq!(new_leader_id,follower_id);
        //----------------------------------- quarum-loss-scenairo---------------------

        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_commit_transactions() {
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

        let (leader_server, _leader_join_handle) = nodes.get(&leader).unwrap();
        //add a mutable transaction to the leader
        let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
        tx.set("foo".to_string(), "bar".to_string());
        let result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

        // wait for the entries to be decided...
        println!("Trasaction committed");

        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 16);
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
        let result: TxResult = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

        // wait for the entries to be decided...
        println!("Trasaction committed");
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 6);
        let last_replicated_tx: example_datastore::Tx = leader_server
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        let oof = leader_server.lock().unwrap().datastore.get_replicated_offset();
        
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
        runtime.shutdown_background();
 
    }
}