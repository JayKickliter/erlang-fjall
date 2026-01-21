use crate::{
    error::{FjallError, FjallOkResult, FjallRes},
    ks::KsRsc,
};
use rustler::{Resource, ResourceArc};
use std::sync::Mutex;

pub mod atom {
    rustler::atoms! {
        ok,
    }
}

////////////////////////////////////////////////////////////////////////////
// Write Batch Resource                                                  //
////////////////////////////////////////////////////////////////////////////

pub struct WbRsc(pub Mutex<Option<fjall::OwnedWriteBatch>>);

#[rustler::resource_impl]
impl Resource for WbRsc {}

impl WbRsc {
    /// Borrow batch mutably for insert/remove operations
    fn with_batch_mut<F, R>(&self, f: F) -> Result<R, FjallError>
    where
        F: FnOnce(&mut fjall::OwnedWriteBatch) -> R,
    {
        let mut inner = self
            .0
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire batch lock".to_string()))?;
        match inner.as_mut() {
            Some(batch) => Ok(f(batch)),
            None => Err(FjallError::BatchAlreadyCommitted),
        }
    }

    /// Take batch for commit (consumes the batch)
    fn take_batch(&self) -> Result<fjall::OwnedWriteBatch, FjallError> {
        let mut inner = self
            .0
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire batch lock".to_string()))?;
        inner.take().ok_or(FjallError::BatchAlreadyCommitted)
    }
}

////////////////////////////////////////////////////////////////////////////
// Write Batch NIFs                                                       //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn wb_insert(
    batch: ResourceArc<WbRsc>,
    ks: ResourceArc<KsRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> rustler::Atom {
    let _ = batch.with_batch_mut(|b| {
        b.insert(&ks.0, key.as_slice(), value.as_slice());
    });
    atom::ok()
}

#[rustler::nif]
pub fn wb_remove(
    batch: ResourceArc<WbRsc>,
    ks: ResourceArc<KsRsc>,
    key: rustler::Binary,
) -> rustler::Atom {
    let _ = batch.with_batch_mut(|b| {
        b.remove(&ks.0, key.as_slice());
    });
    atom::ok()
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn wb_commit(batch: ResourceArc<WbRsc>) -> FjallOkResult {
    let result = (|| {
        let wb = batch.take_batch()?;
        wb.commit().to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif]
pub fn wb_len(batch: ResourceArc<WbRsc>) -> usize {
    batch.with_batch_mut(|b| b.len()).unwrap_or(0)
}

#[rustler::nif]
pub fn wb_is_empty(batch: ResourceArc<WbRsc>) -> bool {
    batch.with_batch_mut(|b| b.is_empty()).unwrap_or(true)
}
