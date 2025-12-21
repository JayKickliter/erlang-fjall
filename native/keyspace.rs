use crate::{
    config::{decode_path, parse_config_options},
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
// NIFs                                                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_nif(
    path: rustler::Binary,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> FjallResult<ResourceArc<KeyspaceRsc>> {
    let result = (|| {
        let path_str = decode_path(path)?;
        let config = parse_config_options(path_str, options)?;
        let keyspace = config.open().to_erlang_result()?;
        Ok(ResourceArc::new(KeyspaceRsc(keyspace)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn persist(keyspace: ResourceArc<KeyspaceRsc>, mode: rustler::Atom) -> FjallOkResult {
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
        keyspace.0.persist(persist_mode).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

////////////////////////////////////////////////////////////////////////////
// Resources                                                              //
////////////////////////////////////////////////////////////////////////////

pub struct KeyspaceRsc(pub fjall::Keyspace);

impl std::panic::RefUnwindSafe for KeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for KeyspaceRsc {}
