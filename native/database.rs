use crate::{
    config::decode_path,
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
};
use rustler::{Resource, ResourceArc};

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

pub struct DatabaseRsc(pub fjall::Database);

impl std::panic::RefUnwindSafe for DatabaseRsc {}

#[rustler::resource_impl]
impl Resource for DatabaseRsc {}

////////////////////////////////////////////////////////////////////////////
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<DatabaseRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let builder = crate::config::parse_builder_options_database(&path_str, options)?;
        let db = builder.open().to_erlang_result()?;
        Ok(ResourceArc::new(DatabaseRsc(db)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn persist(db: ResourceArc<DatabaseRsc>, mode: rustler::Atom) -> FjallOkResult {
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
