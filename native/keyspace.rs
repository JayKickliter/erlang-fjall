use crate::{
    error::{FjallBinaryResult, FjallError, FjallOkResult, FjallRes, FjallResult},
    database::DatabaseRsc,
};
use rustler::{Resource, ResourceArc};

////////////////////////////////////////////////////////////////////////////
// Keyspace Resource (formerly Partition)                                 //
////////////////////////////////////////////////////////////////////////////

pub struct KeyspaceRsc(pub fjall::Keyspace);

impl std::panic::RefUnwindSafe for KeyspaceRsc {}

#[rustler::resource_impl]
impl Resource for KeyspaceRsc {}

////////////////////////////////////////////////////////////////////////////
// Keyspace NIFs                                                          //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_keyspace(
    db: ResourceArc<DatabaseRsc>,
    name: String,
) -> FjallResult<ResourceArc<KeyspaceRsc>> {
    let result = (|| {
        let keyspace =
            db.0.keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .to_erlang_result()?;
        Ok(ResourceArc::new(KeyspaceRsc(keyspace)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn insert(
    keyspace: ResourceArc<KeyspaceRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = (|| {
        keyspace
            .0
            .insert(key.as_slice(), value.as_slice())
            .to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif]
pub fn get(keyspace: ResourceArc<KeyspaceRsc>, key: rustler::Binary) -> FjallBinaryResult {
    let result = (|| {
        let val = keyspace.0.get(key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}

#[rustler::nif]
pub fn remove(keyspace: ResourceArc<KeyspaceRsc>, key: rustler::Binary) -> FjallOkResult {
    let result = (|| {
        keyspace.0.remove(key.as_slice()).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}
