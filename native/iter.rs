use crate::{
    error::{atom, FjallError, FjallResult},
    ks::KsRsc,
    otx_ks::OtxKsRsc,
};
use rustler::{Binary, Encoder, Env, NewBinary, Resource, ResourceArc, Term};
use std::sync::Mutex;

mod atoms {
    rustler::atoms! { done, reverse }
}

fn is_reverse(options: &[rustler::Atom]) -> bool {
    options.iter().any(|opt| *opt == atoms::reverse())
}

////////////////////////////////////////////////////////////////////////////
// Iterator Resource                                                      //
////////////////////////////////////////////////////////////////////////////

enum IterInner {
    Forward(fjall::Iter),
    Reverse(std::iter::Rev<fjall::Iter>),
}

impl Iterator for IterInner {
    type Item = fjall::Guard;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterInner::Forward(iter) => iter.next(),
            IterInner::Reverse(iter) => iter.next(),
        }
    }
}

pub struct IterRsc(Mutex<Option<IterInner>>);

#[rustler::resource_impl]
impl Resource for IterRsc {}

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
pub fn iter_next<'a>(env: Env<'a>, iter: ResourceArc<IterRsc>) -> Term<'a> {
    let mut guard = iter.0.lock().unwrap();
    let Some(itr) = guard.as_mut() else {
        return atoms::done().encode(env);
    };
    match itr.next().map(|g| g.into_inner()) {
        None => {
            *guard = None;
            atoms::done().encode(env)
        }
        Some(Ok((k, v))) => {
            let k = make_binary(env, &k);
            let v = make_binary(env, &v);
            (atom::ok(), (k, v)).encode(env)
        }
        Some(Err(e)) => FjallError::from(e).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn iter_collect<'a>(
    env: Env<'a>,
    iter: ResourceArc<IterRsc>,
    limit: Option<usize>,
) -> Term<'a> {
    let mut guard = iter.0.lock().unwrap();
    let Some(itr) = guard.as_mut() else {
        return (atom::ok(), Vec::<(Binary, Binary)>::new()).encode(env);
    };
    let max = limit.filter(|&n| n > 0).unwrap_or(usize::MAX);
    let mut items: Vec<(Binary<'a>, Binary<'a>)> = Vec::new();
    for _ in 0..max {
        match itr.next().map(|g| g.into_inner()) {
            Some(Ok((k, v))) => {
                items.push((make_binary(env, &k), make_binary(env, &v)));
            }
            Some(Err(e)) => {
                return FjallError::from(e).encode(env);
            }
            None => {
                *guard = None;
                break;
            }
        }
    }
    (atom::ok(), items).encode(env)
}

#[rustler::nif]
pub fn iter_destroy(iter: ResourceArc<IterRsc>) -> rustler::Atom {
    if let Ok(mut guard) = iter.0.lock() {
        *guard = None;
    }
    atom::ok()
}

////////////////////////////////////////////////////////////////////////////
// Helpers                                                                //
////////////////////////////////////////////////////////////////////////////

fn make_binary<'a>(env: Env<'a>, data: &[u8]) -> Binary<'a> {
    let mut bin = NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}
