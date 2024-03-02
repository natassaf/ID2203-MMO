use durability::{OmniLogEntry, OmniPaxosDurability};
pub use omnipaxos_durability::*;
pub use std::sync::mpsc::channel;
use node::{Node, NodeRunner, BUFFER_SIZE, ELECTION_TICK_TIMEOUT};
use omnipaxos::{messages::Message, util::NodeId};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{runtime::Builder, sync::mpsc,};

pub mod datastore;
pub mod durability;
pub mod node;
pub mod utils;

const SERVERS: [NodeId; 3] = [1, 2, 3];

#[allow(clippy::type_complexity)]
fn initialize_channels() -> (
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

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let configuration_id = 1;
    let mut op_server_handles = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialize_channels();

    for pid in SERVERS {
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: ELECTION_TICK_TIMEOUT,
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id,
            nodes: SERVERS.into(),
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };
        let storage: MemoryStorage<OmniLogEntry> = MemoryStorage::default();
        let omni_paxos: Arc<Mutex<OmniPaxosDurability>> = Arc::new(Mutex::new(op_config.build(storage).unwrap()));
        let mut node: Node = Node::new(pid, omni_paxos);
        let mut node_runner: NodeRunner = NodeRunner {
            node: Arc::new(Mutex::new(node)),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: sending_channels.clone(),
        };
    };
    runtime.spawn({
        async move {
            node_runner.run().await;
        }
    });

}