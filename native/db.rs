use crate::{
    config::decode_path,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    ks::KsRsc,
    wb::WbRsc,
};
use fjall::Keyspace;
use rustler::{Resource, ResourceArc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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

pub struct DbRsc {
    db: fjall::Database,
    keyspaces: Mutex<HashMap<Vec<u8>, Arc<Keyspace>>>,
}

impl std::panic::RefUnwindSafe for DbRsc {}

#[rustler::resource_impl]
impl Resource for DbRsc {}

////////////////////////////////////////////////////////////////////////////
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_open(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<DbRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = crate::config::parse_db_options(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(DbRsc {
            db,
            keyspaces: Mutex::new(HashMap::new()),
        }))
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_keyspace(
    db: ResourceArc<DbRsc>,
    name: String,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<KsRsc>> {
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
        Ok(ResourceArc::new(KsRsc(weak)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn db_batch(db: ResourceArc<DbRsc>) -> FjallResult<ResourceArc<WbRsc>> {
    let batch = db.db.batch();
    let res = Ok(ResourceArc::new(WbRsc(Mutex::new(Some(batch)))));
    FjallResult(res)
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
        db.db.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}
