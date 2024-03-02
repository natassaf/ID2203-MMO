pub mod datastore;
pub mod durability;
pub mod node;

use std::{error::Error, thread::spawn};

use node::Node;

fn main() -> Result<(), Box<dyn Error>> {
   newNode = Node.new(1, OmniPaxosDurability::new());
   tokio:spawn()
    Ok(())
}
