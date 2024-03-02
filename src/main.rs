pub mod datastore;
pub mod durability; 
use durability::{OmniLogEntry, OmniPaxosDurability};
pub use omnipaxos_durability::*;
pub use std::sync::mpsc::channel;
pub mod node;
use crate::kv::KVCommand;
use crate::server::Server;
use datastore::example_datastore;
use node::{Node, NodeRunner};
use omnipaxos::{messages::Message, util::NodeId, *};
use omnipaxos_storage::memory_storage::MemoryStorage;
use core::arch;
use std::{collections::HashMap, env, sync::{Arc, Mutex}, thread::spawn};
use tokio::{self, runtime::{self, Builder, Runtime}, sync::mpsc};
const SERVERS: [NodeId; 2] = [1, 2];

type OmniPaxosKV = OmniPaxos<KVCommand, MemoryStorage<KVCommand>>;

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
        cluster_config,
    };
    let op_config_2 = OmniPaxosConfig {
        server_config: server_config_2,
        cluster_config,
    };
    let  storage:MemoryStorage<OmniLogEntry> = MemoryStorage::default();
    let omni_paxos = op_config
        .build(storage)
        .expect("failed to build OmniPaxos");
    let omni_paxos_2 = op_config_2
    .build(storage.clone())
    .expect("failed to build OmniPaxos");
    let mut node: Node = Node::new(1, OmniPaxosDurability::new(omni_paxos));
    let mut node_2: Node = Node::new(2, OmniPaxosDurability::new(omni_paxos_2));
    
    let  (sending_channels, receiver_channels) = initialise_channels();
    

    let  node_runner = NodeRunner {
        node: Arc::new(Mutex::new(node)),
        incoming: receiver_channels.remove(&1).unwrap(),
        outgoing: sending_channels.clone(),
    };
    let  node_runner_2 = NodeRunner {
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
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }
    (sender_channels, receiver_channels)
}