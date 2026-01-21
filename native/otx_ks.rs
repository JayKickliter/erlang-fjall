use crate::error::{
    FjallBinaryResult, FjallError, FjallKvResult, FjallOkResult, FjallRes, FjallResult,
};
use rustler::{Resource, ResourceArc};

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Keyspace Resource                              //
////////////////////////////////////////////////////////////////////////////

pub struct OtxKsRsc(pub fjall::OptimisticTxKeyspace);

impl std::panic::RefUnwindSafe for OtxKsRsc {}

#[rustler::resource_impl]
impl Resource for OtxKsRsc {}

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Keyspace NIFs                                   //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_insert(
    ks: ResourceArc<OtxKsRsc>,
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
pub fn otx_ks_get(ks: ResourceArc<OtxKsRsc>, key: rustler::Binary) -> FjallBinaryResult {
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
pub fn otx_ks_remove(ks: ResourceArc<OtxKsRsc>, key: rustler::Binary) -> FjallOkResult {
    let result = (|| {
        ks.0.remove(key.as_slice()).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_take(ks: ResourceArc<OtxKsRsc>, key: rustler::Binary) -> FjallBinaryResult {
    let result = (|| {
        let val = ks.0.take(key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_contains_key(ks: ResourceArc<OtxKsRsc>, key: rustler::Binary) -> FjallResult<bool> {
    let result = ks.0.contains_key(key.as_slice()).to_erlang_result();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_size_of(ks: ResourceArc<OtxKsRsc>, key: rustler::Binary) -> FjallResult<u32> {
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
pub fn otx_ks_approximate_len(ks: ResourceArc<OtxKsRsc>) -> u64 {
    ks.0.approximate_len() as u64
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_first_key_value(ks: ResourceArc<OtxKsRsc>) -> FjallKvResult {
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
pub fn otx_ks_last_key_value(ks: ResourceArc<OtxKsRsc>) -> FjallKvResult {
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
pub fn otx_ks_path(ks: ResourceArc<OtxKsRsc>) -> String {
    ks.0.path().to_string_lossy().into_owned()
}
