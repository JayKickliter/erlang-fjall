use crate::{
    config::decode_path,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    ks::KsRsc,
    wb::WbRsc,
};
use fjall::{Database, Keyspace};
use rustler::{Resource, ResourceArc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
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

pub struct DbRsc(RwLock<DbRscInner>);

struct DbRscInner {
    db: Option<Database>,
    keyspaces: HashMap<Vec<u8>, Arc<Keyspace>>,
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
        Ok(ResourceArc::new(DbRsc(RwLock::new(DbRscInner {
            db: Some(db),
            keyspaces: HashMap::new(),
        }))))
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
        let mut inner = db.0.write().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        let ks = db_ref.keyspace(&name, || ks_options).to_erlang_result()?;
        let ks = inner
            .keyspaces
            .entry(name.into_bytes())
            .or_insert_with(|| Arc::new(ks));
        let weak = Arc::downgrade(ks);
        Ok(ResourceArc::new(KsRsc(weak)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn db_batch(db: ResourceArc<DbRsc>) -> FjallResult<ResourceArc<WbRsc>> {
    let result = (|| {
        let inner = db.0.read().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        let batch = db_ref.batch();
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
        let inner = db.0.read().unwrap();
        let db_ref = inner.db.as_ref().ok_or(FjallError::DbClosed)?;
        db_ref.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_close(db: ResourceArc<DbRsc>) -> FjallOkResult {
    let mut inner = db.0.write().unwrap();
    inner.keyspaces.clear();
    inner.db.take();
    FjallOkResult(Ok(()))
}
