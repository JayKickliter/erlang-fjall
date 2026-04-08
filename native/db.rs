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

impl DbRsc {
    fn with_inner<F, T>(&self, f: F) -> Result<T, FjallError>
    where
        F: FnOnce(&DbRscInner) -> Result<T, FjallError>,
    {
        let inner = self.0.read().unwrap();
        f(&inner)
    }

    fn with_inner_mut<F, T>(&self, f: F) -> Result<T, FjallError>
    where
        F: FnOnce(&mut DbRscInner) -> Result<T, FjallError>,
    {
        let mut inner = self.0.write().unwrap();
        f(&mut inner)
    }
}

struct DbRscInner {
    db: Option<Database>,
    keyspaces: HashMap<Vec<u8>, Arc<Keyspace>>,
}

impl DbRscInner {
    fn db(&self) -> Result<&Database, FjallError> {
        self.db.as_ref().ok_or(FjallError::DbClosed)
    }
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
        Ok(ResourceArc::new(KsRsc(weak)))
    });
    FjallResult(result)
}

#[rustler::nif]
pub fn db_batch(db: ResourceArc<DbRsc>) -> FjallResult<ResourceArc<WbRsc>> {
    let result = db.with_inner(|inner| {
        let batch = inner.db()?.batch();
        Ok(ResourceArc::new(WbRsc(Mutex::new(Some(batch)))))
    });
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn db_persist(db: ResourceArc<DbRsc>, mode: rustler::Atom) -> FjallOkResult {
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
pub fn db_close(db: ResourceArc<DbRsc>) -> FjallOkResult {
    FjallOkResult(db.with_inner_mut(|inner| {
        inner.keyspaces.clear();
        inner.db.take();
        Ok(())
    }))
}
