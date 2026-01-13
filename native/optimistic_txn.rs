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
pub struct OptimisticTxDatabaseRsc(fjall::OptimisticTxDatabase);

impl std::panic::RefUnwindSafe for OptimisticTxDatabaseRsc {}

#[rustler::resource_impl]
impl Resource for OptimisticTxDatabaseRsc {}

/// Transactional keyspace resource
pub struct OptimisticTxKeyspaceRsc(fjall::OptimisticTxKeyspace);

impl std::panic::RefUnwindSafe for OptimisticTxKeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for OptimisticTxKeyspaceRsc {}

/// Write transaction resource
///
/// OptimisticTxDatabase::WriteTransaction has no lifetime constraints,
/// so we can store it directly without any unsafe code.
pub struct OptimisticWriteTxRsc(Mutex<Option<fjall::OptimisticWriteTx>>);

impl OptimisticWriteTxRsc {
    /// Create a new write transaction
    fn new(db_ref: ResourceArc<OptimisticTxDatabaseRsc>) -> Result<Self, FjallError> {
        let txn = db_ref.0.write_tx().to_erlang_result()?;
        Ok(OptimisticWriteTxRsc(Mutex::new(Some(txn))))
    }

    /// Borrow transaction mutably for read/write operations
    fn with_txn_mut<F, R>(&self, f: F) -> Result<R, FjallError>
    where
        F: FnOnce(&mut fjall::OptimisticWriteTx) -> Result<R, FjallError>,
    {
        let mut inner = self
            .0
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
            .0
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire transaction lock".to_string()))?;
        inner.take().ok_or(FjallError::TransactionAlreadyFinalized)
    }
}

impl std::panic::RefUnwindSafe for OptimisticWriteTxRsc {}

#[rustler::resource_impl]
impl Resource for OptimisticWriteTxRsc {}

impl Drop for OptimisticWriteTxRsc {
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
pub struct SnapshotRsc {
    pub snapshot: fjall::Snapshot,
}

impl std::panic::RefUnwindSafe for SnapshotRsc {}

#[rustler::resource_impl]
impl Resource for SnapshotRsc {}

////////////////////////////////////////////////////////////////////////////
// Database & Keyspace NIFs                                               //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_txn_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<OptimisticTxDatabaseRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = parse_builder_options_optimistic_tx(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(OptimisticTxDatabaseRsc(db)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn open_txn_keyspace(
    db: ResourceArc<OptimisticTxDatabaseRsc>,
    name: String,
) -> FjallResult<ResourceArc<OptimisticTxKeyspaceRsc>> {
    let result = (|| {
        let keyspace =
            db.0.keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .to_erlang_result()?;
        Ok(ResourceArc::new(OptimisticTxKeyspaceRsc(keyspace)))
    })();
    FjallResult(result)
}

////////////////////////////////////////////////////////////////////////////
// Write Transaction NIFs                                                 //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn begin_write_txn(
    db: ResourceArc<OptimisticTxDatabaseRsc>,
) -> FjallResult<ResourceArc<OptimisticWriteTxRsc>> {
    let result = OptimisticWriteTxRsc::new(db).map(ResourceArc::new);
    FjallResult(result)
}

#[rustler::nif]
pub fn txn_insert(
    txn: ResourceArc<OptimisticWriteTxRsc>,
    keyspace: ResourceArc<OptimisticTxKeyspaceRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.insert(&keyspace.0, key.as_slice(), value.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn txn_get(
    txn: ResourceArc<OptimisticWriteTxRsc>,
    keyspace: ResourceArc<OptimisticTxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = txn.with_txn_mut(|t| {
        use fjall::Readable;
        let val = t.get(&keyspace.0, key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    });
    FjallBinaryResult(result)
}

#[rustler::nif]
pub fn txn_remove(
    txn: ResourceArc<OptimisticWriteTxRsc>,
    keyspace: ResourceArc<OptimisticTxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.remove(&keyspace.0, key.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif]
pub fn commit_txn(txn: ResourceArc<OptimisticWriteTxRsc>) -> FjallOkResult {
    let result = (|| {
        let transaction = txn.take_txn()?;
        // commit() returns Result<Result<(), Conflict>, Error>
        // Outer error is IO error, inner error is conflict
        transaction.commit()??;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif]
pub fn rollback_txn(txn: ResourceArc<OptimisticWriteTxRsc>) -> FjallOkResult {
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
pub fn begin_read_txn(
    db: ResourceArc<OptimisticTxDatabaseRsc>,
) -> FjallResult<ResourceArc<SnapshotRsc>> {
    let snapshot = db.0.read_tx();
    let read_txn = SnapshotRsc { snapshot };
    FjallResult(Ok(ResourceArc::new(read_txn)))
}

#[rustler::nif]
pub fn read_txn_get(
    snapshot_rsc: ResourceArc<SnapshotRsc>,
    keyspace: ResourceArc<OptimisticTxKeyspaceRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = (|| {
        use fjall::Readable;
        let val = snapshot_rsc
            .snapshot
            .get(&keyspace.0, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}
