use crate::error::{
    FjallBinaryResult, FjallError, FjallKvResult, FjallOkResult, FjallRes, FjallResult,
};
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

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_contains_key(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallResult<bool> {
    let result = ks.0.contains_key(key.as_slice()).to_erlang_result();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_size_of(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallResult<u32> {
    let result = (|| {
        let size = ks.0.size_of(key.as_slice()).to_erlang_result()?;
        match size {
            Some(s) => Ok(s),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_approximate_len(ks: ResourceArc<KsRsc>) -> u64 {
    ks.0.approximate_len() as u64
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_first_key_value(ks: ResourceArc<KsRsc>) -> FjallKvResult {
    let result = (|| match ks.0.first_key_value() {
        Some(guard) => {
            let (k, v) = guard.into_inner().to_erlang_result()?;
            Ok((k.to_vec(), v.to_vec()))
        }
        None => Err(FjallError::NotFound),
    })();
    FjallKvResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_last_key_value(ks: ResourceArc<KsRsc>) -> FjallKvResult {
    let result = (|| match ks.0.last_key_value() {
        Some(guard) => {
            let (k, v) = guard.into_inner().to_erlang_result()?;
            Ok((k.to_vec(), v.to_vec()))
        }
        None => Err(FjallError::NotFound),
    })();
    FjallKvResult(result)
}

#[rustler::nif]
pub fn ks_path(ks: ResourceArc<KsRsc>) -> String {
    ks.0.path().to_string_lossy().into_owned()
}
