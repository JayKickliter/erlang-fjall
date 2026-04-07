use crate::{
    config::decode_path,
    db::atom,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    otx_ks::OtxKsRsc,
    otx_tx::WriteTxRsc,
    snapshot::SnapshotRsc,
};
use fjall::OptimisticTxKeyspace;
use rustler::{Resource, ResourceArc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Database Resource                              //
////////////////////////////////////////////////////////////////////////////

pub struct OtxDbRsc {
    db: fjall::OptimisticTxDatabase,
    keyspaces: Mutex<HashMap<Vec<u8>, Arc<OptimisticTxKeyspace>>>,
}

impl OtxDbRsc {
    pub fn write_tx(&self) -> Result<fjall::OptimisticWriteTx, FjallError> {
        self.db.write_tx().to_erlang_result()
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
        Ok(ResourceArc::new(OtxDbRsc {
            db,
            keyspaces: Mutex::new(HashMap::new()),
        }))
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
        let ks = db.db.keyspace(&name, || ks_options).to_erlang_result()?;
        let mut keyspaces = db
            .keyspaces
            .lock()
            .map_err(|_| FjallError::Config("Failed to acquire keyspaces lock".into()))?;
        let ks = keyspaces
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
    let snapshot = db.db.read_tx();
    let result = Ok(ResourceArc::new(SnapshotRsc { snapshot }));
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
        db.db.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}
