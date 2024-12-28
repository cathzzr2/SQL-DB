use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    u64,
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{
    engine::Engine,
    keycode::{deserialize_key, serialize_key},
};

pub type Version = u64;

pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

// Transaction state
pub struct TransactionState {
    // current transaction's version no.
    pub version: Version,
    // the list of current active transactions 
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false;
        } else {
            return version <= self.version;
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnAcvtive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}

// NextVersion 0
// TxnAcvtive 1-100 1-101 1-102
// Version key1-101 key2-101

impl MvccKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnAcvtive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

impl<E: Engine> MvccTransaction<E> {
    // Begin a transaction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // Fetch storage engine
        let mut engine = eng.lock()?;
        // Fetch the updated version no.
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };
        // Store the next version
        engine.set(
            MvccKey::NextVersion.encode()?,
            bincode::serialize(&(next_version + 1))?,
        )?;

        // Fetch the list of current active transactions
        let active_versions = Self::scan_active(&mut engine)?;

        // Add current transaction into the list
        engine.set(MvccKey::TxnAcvtive(next_version).encode()?, vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    // commit transaction
    pub fn commit(&self) -> Result<()> {
        // Fetch storage engine
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // Find current transactions TxnWrite
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // remove from the active list
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)
    }

    // roll back transaction
    pub fn rollback(&self) -> Result<()> {
        // fetch storage engine
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // find current transaction's TxnWrite 
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // remove from the active list
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // fetch storage engine
        let mut engine = self.engine.lock()?;

        // version: 9
        // version range: 0-8
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev();
        // read from the updated version, find a newest visible version
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(None)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        // Original           Encoded
        // 97 98 99     -> 97 98 99 0 0
        // Original prefix  Encoded prefix
        // 97 98        -> 97 98 0 0         -> 97 98
        // remove the last [0, 0] suffix
        enc_prefix.truncate(enc_prefix.len() - 2);

        let mut iter = eng.scan_prefix(enc_prefix);
        let mut results = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::deserialize(&value)? {
                            Some(raw_value) => results.insert(raw_key, raw_value),
                            None => results.remove(&raw_key),
                        };
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexepected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(results
            .into_iter()
            .map(|(key, value)| ScanResult { key, value })
            .collect())
    }

    // update/delete data
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // fetch storage engine
        let mut engine = self.engine.lock()?;

        // check conflicts
        //  3 4 5
        //  6
        //  key1-3 key2-4 key3-5
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        // Currently active transactions: 3, 4, 5
        // Current transaction: 6
        // Only need to check the last version number
        // 1. Keys are sorted, so scan results are in ascending order
        // 2. If a new transaction (e.g., 10) modifies the key and commits, 
        //    then if transaction 6 tries to modify that key, it’s a conflict
        // 3. If one of the currently active transactions (e.g., 4) has modified the key, 
        //    then transaction 5 can’t modify it

        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // check if the version is visible
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }

        // record what keys are writtenin to this version
        // will be used for rollback
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode()?,
            vec![],
        )?;

        // write in the actual key value
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode()?,
            bincode::serialize(&value)?,
        )?;
        Ok(())
    }

    // scan/fetch the active list
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnAcvtive.encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnAcvtive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(active_versions)
    }
}

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}