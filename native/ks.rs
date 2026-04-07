use crate::{
    error::{FjallError, FjallOkResult, FjallRes, FjallResult},
    make_binary,
};
use fjall::Keyspace;
use rustler::{types::tuple::make_tuple, Encoder, Env, Resource, ResourceArc, Term};
use std::sync::{Arc, Weak};

////////////////////////////////////////////////////////////////////////////
// Keyspace Resource                                                      //
////////////////////////////////////////////////////////////////////////////

pub struct KsRsc(pub Weak<Keyspace>);

impl KsRsc {
    pub fn upgrade(&self) -> Result<Arc<Keyspace>, FjallError> {
        self.0.upgrade().ok_or(FjallError::DbClosed)
    }
}

impl std::panic::RefUnwindSafe for KsRsc {}

#[rustler::resource_impl]
impl Resource for KsRsc {}

////////////////////////////////////////////////////////////////////////////
// Keyspace NIFs                                                          //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_get<'a>(
    env: Env<'a>,
    ks: ResourceArc<KsRsc>,
    key: rustler::Binary,
) -> FjallResult<Term<'a>> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        let val = ks_ref.get(key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(make_binary(env, &value).encode(env)),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_insert(
    ks: ResourceArc<KsRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        ks_ref
            .insert(key.as_slice(), value.as_slice())
            .to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_remove(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallOkResult {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        ks_ref.remove(key.as_slice()).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_disk_space(ks: ResourceArc<KsRsc>) -> FjallResult<u64> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        Ok(ks_ref.disk_space())
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_clear(ks: ResourceArc<KsRsc>) -> FjallOkResult {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        ks_ref.clear().to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_contains_key(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallResult<bool> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        ks_ref.contains_key(key.as_slice()).to_erlang_result()
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_size_of(ks: ResourceArc<KsRsc>, key: rustler::Binary) -> FjallResult<u32> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        let size = ks_ref.size_of(key.as_slice()).to_erlang_result()?;
        match size {
            Some(s) => Ok(s),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_len(ks: ResourceArc<KsRsc>) -> FjallResult<u64> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        ks_ref.len().map(|n| n as u64).to_erlang_result()
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_approximate_len(ks: ResourceArc<KsRsc>) -> FjallResult<u64> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        Ok(ks_ref.approximate_len() as u64)
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_first_key_value<'a>(env: Env<'a>, ks: ResourceArc<KsRsc>) -> FjallResult<Term<'a>> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        match ks_ref.first_key_value() {
            Some(guard) => {
                let (k, v) = guard.into_inner().to_erlang_result()?;
                let kv = make_tuple(
                    env,
                    &[
                        make_binary(env, &k).encode(env),
                        make_binary(env, &v).encode(env),
                    ],
                );
                Ok(kv)
            }
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_last_key_value<'a>(env: Env<'a>, ks: ResourceArc<KsRsc>) -> FjallResult<Term<'a>> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        match ks_ref.last_key_value() {
            Some(guard) => {
                let (k, v) = guard.into_inner().to_erlang_result()?;
                let kv = make_tuple(
                    env,
                    &[
                        make_binary(env, &k).encode(env),
                        make_binary(env, &v).encode(env),
                    ],
                );
                Ok(kv)
            }
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn ks_path(ks: ResourceArc<KsRsc>) -> FjallResult<String> {
    let result = (|| {
        let ks_ref = ks.upgrade()?;
        Ok(ks_ref.path().to_string_lossy().into_owned())
    })();
    FjallResult(result)
}
