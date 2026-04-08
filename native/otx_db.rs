use crate::{
    config::decode_path,
    db::atom,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    otx_ks::OtxKsRsc,
    otx_tx::WriteTxRsc,
    snapshot::SnapshotRsc,
};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx};
use rustler::{Resource, ResourceArc};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Database Resource                              //
////////////////////////////////////////////////////////////////////////////

pub struct OtxDbRsc(RwLock<OtxDbRscInner>);

struct OtxDbRscInner {
    db: Option<OptimisticTxDatabase>,
    keyspaces: HashMap<Vec<u8>, Arc<OptimisticTxKeyspace>>,
}

impl OtxDbRscInner {
    fn db(&self) -> Result<&OptimisticTxDatabase, FjallError> {
        self.db.as_ref().ok_or(FjallError::DbClosed)
    }
}

impl OtxDbRsc {
    pub fn write_tx(&self) -> Result<OptimisticWriteTx, FjallError> {
        self.with_inner(|inner| inner.db()?.write_tx().to_erlang_result())
    }

    fn with_inner<F, T>(&self, f: F) -> Result<T, FjallError>
    where
        F: FnOnce(&OtxDbRscInner) -> Result<T, FjallError>,
    {
        f(&self.0.read().unwrap())
    }

    fn with_inner_mut<F, T>(&self, f: F) -> Result<T, FjallError>
    where
        F: FnOnce(&mut OtxDbRscInner) -> Result<T, FjallError>,
    {
        f(&mut self.0.write().unwrap())
    }
}

impl std::panic::RefUnwindSafe for OtxDbRsc {}

#[rustler::resource_impl]
impl Resource for OtxDbRsc {}

////////////////////////////////////////////////////////////////////////////
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_open(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<OtxDbRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = crate::config::parse_otx_db_options(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(OtxDbRsc(RwLock::new(OtxDbRscInner {
            db: Some(db),
            keyspaces: HashMap::new(),
        }))))
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_keyspace(
    db: ResourceArc<OtxDbRsc>,
    name: String,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<OtxKsRsc>> {
    let result = db.with_inner_mut(|inner| {
        let ks_options = crate::config::parse_ks_options(options)?;
        let ks = inner
            .db()?
            .keyspace(&name, || ks_options)
            .to_erlang_result()?;
        let ks = inner
            .keyspaces
            .entry(name.into_bytes())
            .or_insert_with(|| Arc::new(ks));
        let weak = Arc::downgrade(ks);
        Ok(ResourceArc::new(OtxKsRsc(weak)))
    });
    FjallResult(result)
}

#[rustler::nif]
pub fn otx_db_write_tx(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<WriteTxRsc>> {
    let result = WriteTxRsc::new(db).map(ResourceArc::new);
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_snapshot(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<SnapshotRsc>> {
    let result = db.with_inner(|inner| {
        let snapshot = inner.db()?.read_tx();
        Ok(ResourceArc::new(SnapshotRsc { snapshot }))
    });
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_persist(db: ResourceArc<OtxDbRsc>, mode: rustler::Atom) -> FjallOkResult {
    let result = db.with_inner(|inner| {
        let persist_mode = if mode == atom::buffer() {
            fjall::PersistMode::Buffer
        } else if mode == atom::sync_data() {
            fjall::PersistMode::SyncData
        } else if mode == atom::sync_all() {
            fjall::PersistMode::SyncAll
        } else {
            return Err(FjallError::Config(format!(
                "Unknown persist mode: {:?}",
                mode
            )));
        };
        inner.db()?.persist(persist_mode).to_erlang_result()?;
        Ok(())
    });
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_close(db: ResourceArc<OtxDbRsc>) -> FjallOkResult {
    FjallOkResult(db.with_inner_mut(|inner| {
        inner.keyspaces.clear();
        inner.db.take();
        Ok(())
    }))
}
