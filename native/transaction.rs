use crate::{
    config::{decode_path, parse_builder_options_optimistic_tx},
    error::{FjallBinaryResult, FjallError, FjallOkResult, FjallRes, FjallResult},
};
use rustler::{Resource, ResourceArc};
use std::sync::Mutex;

pub mod atom {
    rustler::atoms! {
        transaction_conflict,
    }
}

////////////////////////////////////////////////////////////////////////////
// Resources                                                              //
////////////////////////////////////////////////////////////////////////////

/// Transactional database resource (OptimisticTxDatabase)
pub struct TxDatabaseRsc(pub fjall::OptimisticTxDatabase);

impl std::panic::RefUnwindSafe for TxDatabaseRsc {}

#[rustler::resource_impl]
impl Resource for TxDatabaseRsc {}

/// Transactional keyspace resource
pub struct TxKeyspaceRsc {
    pub keyspace: fjall::OptimisticTxKeyspace,
    // Keep database alive during keyspace lifetime
    _db_ref: ResourceArc<TxDatabaseRsc>,
}

impl std::panic::RefUnwindSafe for TxKeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for TxKeyspaceRsc {}

/// Write transaction resource
///
/// OptimisticTxDatabase::WriteTransaction has no lifetime constraints,
/// so we can store it directly without any unsafe code.
pub struct WriteTxnRsc {
    txn: Mutex<Option<fjall::OptimisticWriteTx>>,
    // Keep database alive during transaction lifetime
    _db_ref: ResourceArc<TxDatabaseRsc>,
}

impl WriteTxnRsc {
    /// Create a new write transaction
    fn new(db_ref: ResourceArc<TxDatabaseRsc>) -> Result<Self, FjallError> {
        let txn = db_ref.0.write_tx().to_erlang_result()?;
        Ok(WriteTxnRsc {
            txn: Mutex::new(Some(txn)),
            _db_ref: db_ref,
        })
    }

    /// Borrow transaction mutably for read/write operations
    fn with_txn_mut<F, R>(&self, f: F) -> Result<R, FjallError>
    where
        F: FnOnce(&mut fjall::OptimisticWriteTx) -> Result<R, FjallError>,
    {
        let mut inner = self
            .txn
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire transaction lock".to_string()))?;
        match inner.as_mut() {
            Some(txn) => f(txn),
            None => Err(FjallError::TransactionAlreadyFinalized),
        }
    }

    /// Take transaction for commit/rollback (consumes the transaction)
    fn take_txn(&self) -> Result<fjall::OptimisticWriteTx, FjallError> {
        let mut inner = self
            .txn
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire transaction lock".to_string()))?;
        inner.take().ok_or(FjallError::TransactionAlreadyFinalized)
    }
}

impl std::panic::RefUnwindSafe for WriteTxnRsc {}

#[rustler::resource_impl]
impl Resource for WriteTxnRsc {}

impl Drop for WriteTxnRsc {
    fn drop(&mut self) {
        // Automatically rollback if transaction wasn't explicitly
        // committed/rolled back. We ignore any errors during drop.
        let _ = self.take_txn();
    }
}

/// Read transaction resource
///
/// In fjall 3.0, read transactions are called Snapshot and provide
/// consistent point-in-time views of the data.
pub struct ReadTxnRsc {
    pub snapshot: fjall::Snapshot,
    // Keep database alive during snapshot lifetime
    _db_ref: ResourceArc<TxDatabaseRsc>,
}

impl std::panic::RefUnwindSafe for ReadTxnRsc {}

#[rustler::resource_impl]
impl Resource for ReadTxnRsc {}

////////////////////////////////////////////////////////////////////////////
// Database & Keyspace NIFs                                               //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_txn_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<TxDatabaseRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = parse_builder_options_optimistic_tx(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(TxDatabaseRsc(db)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn open_txn_keyspace(
    db: ResourceArc<TxDatabaseRsc>,
    name: String,
) -> FjallResult<ResourceArc<TxKeyspaceRsc>> {
    let result = (|| {
        let keyspace =
            db.0.keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .to_erlang_result()?;
        Ok(ResourceArc::new(TxKeyspaceRsc {
            keyspace,
            _db_ref: db,
        }))
    })();
    FjallResult(result)
}

////////////////////////////////////////////////////////////////////////////
// Write Transaction NIFs                                                 //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn begin_write_txn(db: ResourceArc<TxDatabaseRsc>) -> FjallResult<ResourceArc<WriteTxnRsc>> {
    let result = WriteTxnRsc::new(db).map(ResourceArc::new);
    FjallResult(result)
}

#[rustler::nif]
pub fn txn_insert(
    txn: ResourceArc<WriteTxnRsc>,
    keyspace: ResourceArc<TxKeyspaceRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.insert(&keyspace.keyspace, key.as_slice(), value.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn txn_get(
    txn: ResourceArc<WriteTxnRsc>,
    keyspace: ResourceArc<TxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = txn.with_txn_mut(|t| {
        use fjall::Readable;
        let val = t
            .get(&keyspace.keyspace, key.as_slice())
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
    keyspace: ResourceArc<TxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.remove(&keyspace.keyspace, key.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn commit_txn(txn: ResourceArc<WriteTxnRsc>) -> FjallOkResult {
    let result = (|| {
        let transaction = txn.take_txn()?;
        let _ = transaction.commit().to_erlang_result()?;
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
// Read Transaction (Snapshot) NIFs                                       //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn begin_read_txn(db: ResourceArc<TxDatabaseRsc>) -> FjallResult<ResourceArc<ReadTxnRsc>> {
    let snapshot = db.0.read_tx();
    let read_txn = ReadTxnRsc {
        snapshot,
        _db_ref: db,
    };
    FjallResult(Ok(ResourceArc::new(read_txn)))
}

#[rustler::nif]
pub fn read_txn_get(
    snapshot_rsc: ResourceArc<ReadTxnRsc>,
    keyspace: ResourceArc<TxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = (|| {
        use fjall::Readable;
        let val = snapshot_rsc
            .snapshot
            .get(&keyspace.keyspace, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}
