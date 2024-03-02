pub mod datastore;
pub mod durability; 
use durability::{omnipaxos_durability, OmniLogEntry, OmniPaxosDurability};
pub use omnipaxos_durability::*;
pub use std::sync::mpsc::channel;
pub mod node;
use node::{Node, NodeRunner};
use omnipaxos::{messages::Message, util::NodeId, *};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{collections::HashMap, sync::{Arc, Mutex}};
use tokio::{self, runtime::{ Builder, Runtime }, sync::mpsc};
const SERVERS: [NodeId; 2] = [1, 2];

type OmniPaxosKV = OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>;

fn main() {
    let server_config = ServerConfig {
        pid: 1,
        election_tick_timeout: 5,
        ..Default::default()
    };
    let server_config_2 = ServerConfig {
        pid: 2,
        election_tick_timeout: 5,
        ..Default::default()
    };
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1,2],// PIDS of all nodes
        ..Default::default()
    };
    let op_config = OmniPaxosConfig {
        server_config,
        cluster_config: cluster_config.clone(),
    };
    let op_config_2 = OmniPaxosConfig {
        server_config: server_config_2,
        cluster_config,
    };
    let  storage:MemoryStorage<OmniLogEntry> = MemoryStorage::default();
    let omni_paxos = op_config
        .build(storage.clone())
        .expect("failed to build OmniPaxos");
    let omni_paxos_2 = op_config_2
    .build(storage)
    .expect("failed to build OmniPaxos");
    let mut node: Node = Node::new(1, OmniPaxosDurability::new(omni_paxos));
    let mut node_2: Node = Node::new(2, OmniPaxosDurability::new(omni_paxos_2));
    
    let  (sending_channels, mut receiver_channels) = initialise_channels();
    

    let mut  node_runner = NodeRunner {
        node: Arc::new(Mutex::new(node)),
        incoming: receiver_channels.remove(&1).unwrap(),
        outgoing: sending_channels.clone(),
    };
    let mut  node_runner_2 = NodeRunner {
        node: Arc::new(Mutex::new(node_2)),
        incoming: receiver_channels.remove(&2).unwrap(),
        outgoing: sending_channels.clone(),
    };
    let runtime= create_runtime();
    runtime.spawn(async move{
    _ = node_runner.run().await;
    });
    runtime.spawn(async move{
        _ = node_runner_2.run().await;
        });

}
fn create_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}


#[allow(clippy::type_complexity)]
fn initialise_channels() -> (
    HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
    HashMap<NodeId, mpsc::Receiver<Message<OmniLogEntry>>>,
) {
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();

    for pid in SERVERS {
        let (sender, receiver) = mpsc::channel(4);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }
    (sender_channels, receiver_channels)
}