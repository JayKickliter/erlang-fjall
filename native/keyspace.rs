use rustler::{Resource, ResourceArc};
use crate::config::{decode_path, parse_config_options};
use crate::error::{FjallResult, FjallRes};

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

////////////////////////////////////////////////////////////////////////////
// Resources                                                              //
////////////////////////////////////////////////////////////////////////////

pub struct KeyspaceRsc(pub fjall::Keyspace);

impl std::panic::RefUnwindSafe for KeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for KeyspaceRsc {}

pub struct PartitionRsc(pub fjall::PartitionHandle);

impl std::panic::RefUnwindSafe for PartitionRsc {}

#[rustler::resource_impl]
impl Resource for PartitionRsc {}
