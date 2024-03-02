pub mod datastore;
pub mod durability;
pub mod node;
use crate::kv::KVCommand;
use crate::server::Server;
use datastore::example_datastore;
use node::{Node, NodeRunner};
use omnipaxos::*;
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::env;
use tokio;



type OmniPaxosKV = OmniPaxos<KVCommand, MemoryStorage<KVCommand>>;

fn main() {
    let server_config = ServerConfig {
        pid: None,
        election_tick_timeout: 5,
        ..Default::default()
    };
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: None,// PIDS of all nodes
        ..Default::default()
    };
    let op_config = OmniPaxosConfig {
        server_config,
        cluster_config,
    };
    let omni_paxos = op_config
        .build(MemoryStorage::default())
        .expect("failed to build OmniPaxos");
    let mut node = Node::new(1, dur);
    NodeRunner::new(&mut node).run();
}
