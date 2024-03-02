use crate::datastore::tx_data;

use super::*;
use omnipaxos::{macros::Entry, OmniPaxos};
use omnipaxos_storage::memory_storage::MemoryStorage;
use omnipaxos::util::LogEntry as utilLogEntry;
use std::{
    fs::File,
    io::{Read, Write},
    sync::{Arc, Mutex},
};

#[derive(Entry, Clone, Debug)]
pub struct OmniLogEntry {
    tx_offset: TxOffset,
    tx_data: TxData,
}

impl OmniLogEntry {
    fn new(tx_offset: TxOffset, tx_data:TxData)->Self{
        OmniLogEntry{tx_offset, tx_data}
    }

    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.tx_offset.0.to_le_bytes())?;
        self.tx_data.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        let tx_offset = TxOffset(u64::from_le_bytes(bytes));
        let tx_data = TxData::deserialize(reader)?;
        Ok(OmniLogEntry { tx_offset, tx_data })
    }
}

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    pub omnipaxos: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>
}

impl OmniPaxosDurability{

    pub fn new(omnipaxos: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>)->Self{
        OmniPaxosDurability{omnipaxos}
    }
    
    fn decode_log_entry_decided(log_entry:utilLogEntry<OmniLogEntry>)-> Option<(TxOffset, TxData)>{
        match log_entry {
            utilLogEntry::Decided(entry) => Some((entry.tx_offset.clone(), entry.tx_data.clone())),
            _ => None,
        }
    }
} 

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // iterate over the decided log entries
        let log_iter = self.omnipaxos.read_entries(0..self.omnipaxos.get_decided_idx());
        let decided_entries: Vec<(TxOffset, TxData)> = log_iter.unwrap().iter().filter_map(|log_entry| {
            Self::decode_log_entry_decided(log_entry.clone())
        }).collect();

        Box::new(decided_entries.into_iter())
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let entries_after_offset = match self.omnipaxos.read_decided_suffix(offset.0){
            Some(entries)=>entries.clone(),
            None=>vec![]
        };
        let decided_entries: Vec<(TxOffset, TxData)> = entries_after_offset.iter().filter_map(|log_entry| {
            Self::decode_log_entry_decided(log_entry.clone())
        }).collect();

        Box::new(decided_entries.into_iter())    
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        let log_entry = OmniLogEntry::new(tx_offset, tx_data);
        match self.omnipaxos.append(log_entry){
            Ok(res)=> println!("entry appended successfully! {:?}", res),
            Err(e)=> println!("Error in appending {:?}", e)
        };
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        self
        .iter()
        .last()
        .map(|(tx_offset, _)| tx_offset)
        .unwrap_or(TxOffset(0))
    }
}
