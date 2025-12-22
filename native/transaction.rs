use crate::{
    config::{decode_path, parse_config_options},
    error::{FjallBinaryResult, FjallError, FjallOkResult, FjallRes, FjallResult},
};
use rustler::{Resource, ResourceArc};
use std::sync::Mutex;

pub mod atom {
    rustler::atoms! {
        transaction_already_finalized,
        transaction_conflict,
    }
}

////////////////////////////////////////////////////////////////////////////
// Resources                                                              //
////////////////////////////////////////////////////////////////////////////

/// Transactional keyspace resource
pub struct TxnKeyspaceRsc(pub fjall::TransactionalKeyspace);

impl std::panic::RefUnwindSafe for TxnKeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for TxnKeyspaceRsc {}

/// Partition handle in transactional keyspace
pub struct TxnPartitionRsc {
    pub handle: fjall::TxPartitionHandle,
    // Keep keyspace alive during partition lifetime
    _keyspace_ref: ResourceArc<TxnKeyspaceRsc>,
}

impl std::panic::RefUnwindSafe for TxnPartitionRsc {}

#[rustler::resource_impl]
impl Resource for TxnPartitionRsc {}

/// Write transaction resource
pub struct WriteTxnRsc {
    // Mutex for interior mutability and thread safety, Option to track finalization state
    txn: Mutex<Option<fjall::WriteTransaction<'static>>>,
    // Keep keyspace alive during transaction lifetime
    _keyspace_ref: ResourceArc<TxnKeyspaceRsc>,
}

impl WriteTxnRsc {
    /// Create a new write transaction
    fn new(keyspace: ResourceArc<TxnKeyspaceRsc>) -> Self {
        let txn = keyspace.0.write_tx();

        // SAFETY: We transmute the WriteTransaction's lifetime to
        // 'static, but this is safe because we store an Arc to the
        // keyspace. The Arc keeps the keyspace alive, ensuring the
        // 'static lifetime is valid. The keyspace will outlive the
        // transaction because Erlang's GC ensures this resource is
        // dropped before the keyspace.
        let txn_static = unsafe {
            std::mem::transmute::<fjall::WriteTransaction<'_>, fjall::WriteTransaction<'static>>(
                txn,
            )
        };

        WriteTxnRsc {
            txn: Mutex::new(Some(txn_static)),
            _keyspace_ref: keyspace,
        }
    }

    /// Borrow transaction mutably for read/write operations
    fn with_txn_mut<F, R>(&self, f: F) -> Result<R, FjallError>
    where
        F: FnOnce(&mut fjall::WriteTransaction<'static>) -> Result<R, FjallError>,
    {
        let mut txn_opt = self
            .txn
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire transaction lock".to_string()))?;
        match txn_opt.as_mut() {
            Some(txn) => f(txn),
            None => Err(FjallError::TransactionAlreadyFinalized),
        }
    }

    /// Take transaction for commit/rollback (consumes the transaction)
    fn take_txn(&self) -> Result<fjall::WriteTransaction<'static>, FjallError> {
        let mut txn_opt = self
            .txn
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire transaction lock".to_string()))?;
        txn_opt
            .take()
            .ok_or(FjallError::TransactionAlreadyFinalized)
    }
}

impl std::panic::RefUnwindSafe for WriteTxnRsc {}

// SAFETY: WriteTxnRsc is safe to Send because:
// - The WriteTransaction inside only holds internal Mutex guards that are not actually shared
// - We never hand this off to other threads manually
unsafe impl Send for WriteTxnRsc {}

// SAFETY: WriteTxnRsc is safe to Sync because:
// - It's always accessed from within a Rustler NIF, which is single-threaded
// - The internal Mutex is only locked within our with_txn_mut method
unsafe impl Sync for WriteTxnRsc {}

#[rustler::resource_impl]
impl Resource for WriteTxnRsc {}

impl Drop for WriteTxnRsc {
    fn drop(&mut self) {
        // Automatically rollback if transaction wasn't explicitly committed/rolled back
        if let Ok(mut txn_opt) = self.txn.lock() {
            if let Some(txn) = txn_opt.take() {
                txn.rollback();
            }
        }
    }
}

/// Read transaction resource
///
/// Provides snapshot isolation: the transaction sees a consistent
/// point-in-time view of the data. ReadTransaction has no lifetime
/// constraints, so this is simpler than WriteTxnRsc.
pub struct ReadTxnRsc {
    pub txn: fjall::ReadTransaction,
    // Keep keyspace alive during transaction lifetime
    _keyspace_ref: ResourceArc<TxnKeyspaceRsc>,
}

impl std::panic::RefUnwindSafe for ReadTxnRsc {}

#[rustler::resource_impl]
impl Resource for ReadTxnRsc {}

////////////////////////////////////////////////////////////////////////////
// Keyspace NIFs                                                          //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_txn_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<TxnKeyspaceRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let config = parse_config_options(path_str, options)?;
        let keyspace = config.open_transactional().to_erlang_result()?;
        Ok(ResourceArc::new(TxnKeyspaceRsc(keyspace)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn open_txn_partition(
    keyspace: ResourceArc<TxnKeyspaceRsc>,
    name: String,
) -> FjallResult<ResourceArc<TxnPartitionRsc>> {
    let result = (|| {
        let handle = keyspace
            .0
            .open_partition(&name, Default::default())
            .to_erlang_result()?;
        Ok(ResourceArc::new(TxnPartitionRsc {
            handle,
            _keyspace_ref: keyspace,
        }))
    })();
    FjallResult(result)
}

////////////////////////////////////////////////////////////////////////////
// Write Transaction NIFs                                                 //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn begin_write_txn(
    keyspace: ResourceArc<TxnKeyspaceRsc>,
) -> FjallResult<ResourceArc<WriteTxnRsc>> {
    let txn = WriteTxnRsc::new(keyspace);
    FjallResult(Ok(ResourceArc::new(txn)))
}

#[rustler::nif]
pub fn txn_insert(
    txn: ResourceArc<WriteTxnRsc>,
    partition: ResourceArc<TxnPartitionRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        // WriteTransaction insert doesn't return a Result, it modifies in place
        t.insert(&partition.handle, key.as_slice(), value.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn txn_get(
    txn: ResourceArc<WriteTxnRsc>,
    partition: ResourceArc<TxnPartitionRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = txn.with_txn_mut(|t| {
        let val = t
            .get(&partition.handle, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    });
    FjallBinaryResult(result)
}

#[rustler::nif]
pub fn txn_remove(
    txn: ResourceArc<WriteTxnRsc>,
    partition: ResourceArc<TxnPartitionRsc>,
    key: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        // WriteTransaction remove doesn't return a Result, it modifies in place
        t.remove(&partition.handle, key.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn commit_txn(txn: ResourceArc<WriteTxnRsc>) -> FjallOkResult {
    let result = (|| {
        let transaction = txn.take_txn()?;
        transaction.commit().to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif]
pub fn rollback_txn(txn: ResourceArc<WriteTxnRsc>) -> FjallOkResult {
    let result = (|| {
        let _transaction = txn.take_txn()?;
        // Rollback happens automatically when transaction is dropped
        Ok(())
    })();
    FjallOkResult(result)
}

////////////////////////////////////////////////////////////////////////////
// Read Transaction NIFs                                                  //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn begin_read_txn(
    keyspace: ResourceArc<TxnKeyspaceRsc>,
) -> FjallResult<ResourceArc<ReadTxnRsc>> {
    let txn = keyspace.0.read_tx();
    let read_txn = ReadTxnRsc {
        txn,
        _keyspace_ref: keyspace,
    };
    FjallResult(Ok(ResourceArc::new(read_txn)))
}

#[rustler::nif]
pub fn read_txn_get(
    txn: ResourceArc<ReadTxnRsc>,
    partition: ResourceArc<TxnPartitionRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = (|| {
        let val = txn
            .txn
            .get(&partition.handle, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}
