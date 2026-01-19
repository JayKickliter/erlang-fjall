use crate::{
    config::decode_path,
    db::atom,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    otx_ks::OtxKsRsc,
    otx_tx::WriteTxRsc,
    snapshot::SnapshotRsc,
};
use rustler::{Resource, ResourceArc};

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Database Resource                              //
////////////////////////////////////////////////////////////////////////////

pub struct OtxDbRsc(pub fjall::OptimisticTxDatabase);

impl std::panic::RefUnwindSafe for OtxDbRsc {}

#[rustler::resource_impl]
impl Resource for OtxDbRsc {}

////////////////////////////////////////////////////////////////////////////
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn otx_db_open_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<OtxDbRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = crate::config::parse_otx_db_options(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(OtxDbRsc(db)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn otx_db_keyspace(
    db: ResourceArc<OtxDbRsc>,
    name: String,
    _options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<OtxKsRsc>> {
    let result = (|| {
        let ks = db
            .0
            .keyspace(&name, fjall::KeyspaceCreateOptions::default)
            .to_erlang_result()?;
        Ok(ResourceArc::new(OtxKsRsc(ks)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn otx_db_write_tx(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<WriteTxRsc>> {
    let result = WriteTxRsc::new(db).map(ResourceArc::new);
    FjallResult(result)
}

#[rustler::nif]
pub fn otx_db_snapshot(db: ResourceArc<OtxDbRsc>) -> FjallResult<ResourceArc<SnapshotRsc>> {
    let snapshot = db.0.read_tx();
    let result = Ok(ResourceArc::new(SnapshotRsc { snapshot }));
    FjallResult(result)
}

#[rustler::nif]
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
        db.0.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}
