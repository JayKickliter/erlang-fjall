use crate::{
    error::{atom, FjallError, FjallResult},
    ks::KsRsc,
    otx_ks::OtxKsRsc,
};
use rustler::{Encoder, Env, Resource, ResourceArc, Term};
use std::sync::Mutex;

mod iter_atom {
    rustler::atoms! { done, reverse }
}

fn is_reverse(options: &[rustler::Atom]) -> bool {
    options.iter().any(|opt| *opt == iter_atom::reverse())
}

////////////////////////////////////////////////////////////////////////////
// Iterator Resource                                                      //
////////////////////////////////////////////////////////////////////////////

enum IterInner {
    Forward(fjall::Iter),
    Reverse(std::iter::Rev<fjall::Iter>),
}

impl IterInner {
    #[allow(clippy::type_complexity)]
    fn next_item(&mut self) -> Option<Result<(Vec<u8>, Vec<u8>), fjall::Error>> {
        let guard = match self {
            IterInner::Forward(it) => it.next(),
            IterInner::Reverse(it) => it.next(),
        }?;
        Some(guard.into_inner().map(|(k, v)| (k.to_vec(), v.to_vec())))
    }
}

pub struct IterRsc(Mutex<Option<IterInner>>);

impl std::panic::RefUnwindSafe for IterRsc {}

#[rustler::resource_impl]
impl Resource for IterRsc {}

////////////////////////////////////////////////////////////////////////////
// Iterator Result Encoding                                               //
////////////////////////////////////////////////////////////////////////////

pub enum IterValue {
    Item(Vec<u8>, Vec<u8>),
    Batch(Vec<(Vec<u8>, Vec<u8>)>),
    Done,
}

pub struct IterResult(pub Result<IterValue, FjallError>);

impl std::panic::RefUnwindSafe for IterResult {}

impl Encoder for IterResult {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            Ok(IterValue::Item(k, v)) => {
                let key = make_binary(env, k);
                let val = make_binary(env, v);
                (atom::ok(), (key, val)).encode(env)
            }
            Ok(IterValue::Batch(items)) => {
                let pairs: Vec<_> = items
                    .iter()
                    .map(|(k, v)| (make_binary(env, k), make_binary(env, v)))
                    .collect();
                (atom::ok(), pairs).encode(env)
            }
            Ok(IterValue::Done) => iter_atom::done().encode(env),
            Err(err) => err.encode(env),
        }
    }
}

fn make_binary<'a>(env: Env<'a>, data: &[u8]) -> rustler::Binary<'a> {
    let mut bin = rustler::OwnedBinary::new(data.len()).unwrap();
    bin.as_mut_slice().copy_from_slice(data);
    bin.release(env)
}

////////////////////////////////////////////////////////////////////////////
// Iterator Creation - Plain Keyspace                                     //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_iter(
    ks: ResourceArc<KsRsc>,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let iter = ks.0.iter();
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_range(
    ks: ResourceArc<KsRsc>,
    start: rustler::Binary,
    end: rustler::Binary,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let iter = ks.0.range(start.as_slice()..end.as_slice());
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_prefix(
    ks: ResourceArc<KsRsc>,
    prefix: rustler::Binary,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let iter = ks.0.prefix(prefix.as_slice());
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

////////////////////////////////////////////////////////////////////////////
// Iterator Creation - OTX Keyspace                                       //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_iter(
    ks: ResourceArc<OtxKsRsc>,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let iter = keyspace.iter();
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_range(
    ks: ResourceArc<OtxKsRsc>,
    start: rustler::Binary,
    end: rustler::Binary,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let iter = keyspace.range(start.as_slice()..end.as_slice());
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_prefix(
    ks: ResourceArc<OtxKsRsc>,
    prefix: rustler::Binary,
    options: Vec<rustler::Atom>,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let iter = keyspace.prefix(prefix.as_slice());
    let inner = if is_reverse(&options) {
        IterInner::Reverse(iter.rev())
    } else {
        IterInner::Forward(iter)
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

////////////////////////////////////////////////////////////////////////////
// Iterator Operations                                                    //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn iter_next(iter: ResourceArc<IterRsc>) -> IterResult {
    let mut guard = iter.0.lock().unwrap();
    let result = match guard.as_mut() {
        Some(inner) => match inner.next_item() {
            Some(Ok((k, v))) => Ok(IterValue::Item(k, v)),
            Some(Err(e)) => Err(FjallError::from(e)),
            None => {
                // Iterator exhausted - drop it to release resources
                *guard = None;
                Ok(IterValue::Done)
            }
        },
        None => Ok(IterValue::Done),
    };
    IterResult(result)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn iter_collect(iter: ResourceArc<IterRsc>, limit: usize) -> IterResult {
    let mut guard = iter.0.lock().unwrap();
    let result = match guard.as_mut() {
        Some(inner) => {
            let mut items = Vec::new();
            let max = if limit == 0 { usize::MAX } else { limit };
            let mut exhausted = false;
            let mut error: Option<FjallError> = None;
            for _ in 0..max {
                match inner.next_item() {
                    Some(Ok((k, v))) => {
                        items.push((k, v));
                    }
                    Some(Err(e)) => {
                        error = Some(FjallError::from(e));
                        break;
                    }
                    None => {
                        exhausted = true;
                        break;
                    }
                }
            }
            if let Some(e) = error {
                return IterResult(Err(e));
            }
            // If exhausted, drop the iterator to release resources
            if exhausted {
                *guard = None;
            }
            Ok(IterValue::Batch(items))
        }
        None => Ok(IterValue::Batch(Vec::new())),
    };
    IterResult(result)
}

#[rustler::nif]
pub fn iter_destroy(iter: ResourceArc<IterRsc>) -> rustler::Atom {
    if let Ok(mut guard) = iter.0.lock() {
        *guard = None;
    }
    atom::ok()
}
