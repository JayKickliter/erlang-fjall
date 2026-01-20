use crate::error::{FjallBinaryResult, FjallError, FjallOkResult, FjallRes};
use rustler::{Resource, ResourceArc};

////////////////////////////////////////////////////////////////////////////
// Keyspace Resource                                                      //
////////////////////////////////////////////////////////////////////////////

pub struct KsRsc(pub fjall::Keyspace);

impl std::panic::RefUnwindSafe for KsRsc {}

#[rustler::resource_impl]
impl Resource for KsRsc {}

////////////////////////////////////////////////////////////////////////////
// Keyspace NIFs                                                          //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_get(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallBinaryResult {
    let result = (|| {
        let val = ks.0.get(key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_insert(
    ks: ResourceArc<KsRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = (|| {
        ks.0.insert(key.as_slice(), value.as_slice())
            .to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_remove(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallOkResult {
    let result = (|| {
        ks.0.remove(key.as_slice()).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_disk_space(ks: ResourceArc<KsRsc>) -> u64 {
    ks.0.disk_space()
}
