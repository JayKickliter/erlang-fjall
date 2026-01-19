use crate::{
    config::decode_path,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    ks::KsRsc,
    wb::WbRsc,
};
use rustler::{Resource, ResourceArc};
use std::sync::Mutex;

pub mod atom {
    rustler::atoms! {
        buffer,
        sync_data,
        sync_all,
    }
}

////////////////////////////////////////////////////////////////////////////
// Database Resource                                                      //
////////////////////////////////////////////////////////////////////////////

pub struct DbRsc(pub fjall::Database);

impl std::panic::RefUnwindSafe for DbRsc {}

#[rustler::resource_impl]
impl Resource for DbRsc {}

////////////////////////////////////////////////////////////////////////////
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_open_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<DbRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = crate::config::parse_db_options(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(DbRsc(db)))
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_keyspace(
    db: ResourceArc<DbRsc>,
    name: String,
    _options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<KsRsc>> {
    let result = (|| {
        let ks =
            db.0.keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .to_erlang_result()?;
        Ok(ResourceArc::new(KsRsc(ks)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn db_batch(db: ResourceArc<DbRsc>) -> FjallResult<ResourceArc<WbRsc>> {
    let result = (|| {
        let batch = db.0.batch();
        Ok(ResourceArc::new(WbRsc(Mutex::new(Some(batch)))))
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_persist(db: ResourceArc<DbRsc>, mode: rustler::Atom) -> FjallOkResult {
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
