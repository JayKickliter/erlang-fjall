use crate::{
    error::{FjallBinaryResult, FjallError, FjallOkResult, FjallRes},
    otx_db::OtxDbRsc,
    otx_ks::OtxKsRsc,
};
use rustler::{Resource, ResourceArc};
use std::sync::Mutex;

////////////////////////////////////////////////////////////////////////////
// Write Transaction Resource                                            //
////////////////////////////////////////////////////////////////////////////

pub struct WriteTxRsc(pub Mutex<Option<fjall::OptimisticWriteTx>>);

impl std::panic::RefUnwindSafe for WriteTxRsc {}

#[rustler::resource_impl]
impl Resource for WriteTxRsc {}

impl WriteTxRsc {
    /// Create a new write transaction
    pub fn new(db_ref: ResourceArc<OtxDbRsc>) -> Result<Self, FjallError> {
        let txn = db_ref.0.write_tx().to_erlang_result()?;
        Ok(WriteTxRsc(Mutex::new(Some(txn))))
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

impl Drop for WriteTxRsc {
    fn drop(&mut self) {
        // Automatically rollback if transaction wasn't explicitly
        // committed/rolled back. We ignore any errors during drop.
        let _ = self.take_txn();
    }
}

////////////////////////////////////////////////////////////////////////////
// Write Transaction NIFs                                                //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn otx_tx_insert(
    txn: ResourceArc<WriteTxRsc>,
    ks: ResourceArc<OtxKsRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.insert(&ks.0, key.as_slice(), value.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_tx_get(
    txn: ResourceArc<WriteTxRsc>,
    ks: ResourceArc<OtxKsRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = txn.with_txn_mut(|t| {
        use fjall::Readable;
        let val = t.get(&ks.0, key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    });
    FjallBinaryResult(result)
}

#[rustler::nif]
pub fn otx_tx_remove(
    txn: ResourceArc<WriteTxRsc>,
    ks: ResourceArc<OtxKsRsc>,
    key: rustler::Binary,
) -> FjallOkResult {
    let result = txn.with_txn_mut(|t| {
        t.remove(&ks.0, key.as_slice());
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_tx_commit(txn: ResourceArc<WriteTxRsc>) -> FjallOkResult {
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
pub fn otx_tx_rollback(txn: ResourceArc<WriteTxRsc>) -> FjallOkResult {
    let result = (|| {
        let _transaction = txn.take_txn()?;
        // Rollback happens automatically when transaction is dropped
        Ok(())
    })();
    FjallOkResult(result)
}
