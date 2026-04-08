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

impl OtxDbRsc {
    pub fn write_tx(&self) -> Result<OptimisticWriteTx, FjallError> {
        let inner = self.0.read().unwrap();
        let db = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        db.write_tx().to_erlang_result()
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
    let result = (|| {
        let ks_options = crate::config::parse_ks_options(options)?;
        let mut inner = db.0.write().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        let ks = db_ref.keyspace(&name, || ks_options).to_erlang_result()?;
        let ks = inner
            .keyspaces
            .entry(name.into_bytes())
            .or_insert_with(|| Arc::new(ks));
        let weak = Arc::downgrade(ks);
        Ok(ResourceArc::new(OtxKsRsc(weak)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn otx_db_write_tx(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<WriteTxRsc>> {
    let result = WriteTxRsc::new(db).map(ResourceArc::new);
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_snapshot(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<SnapshotRsc>> {
    let result = (|| {
        let inner = db.0.read().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        let snapshot = db_ref.read_tx();
        Ok(ResourceArc::new(SnapshotRsc { snapshot }))
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_persist(db: ResourceArc<OtxDbRsc>, mode: rustler::Atom) -> FjallOkResult {
    let result = (|| {
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
        let inner = db.0.read().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        db_ref.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_db_close(db: ResourceArc<OtxDbRsc>) -> FjallOkResult {
    let mut inner = db.0.write().unwrap();
    inner.keyspaces.clear();
    inner.db.take();
    FjallOkResult(Ok(()))
}
