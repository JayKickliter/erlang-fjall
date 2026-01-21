use crate::{
    error::{atom, FjallError, FjallResult},
    ks::KsRsc,
    make_binary,
    otx_ks::OtxKsRsc,
};
use rustler::{
    types::tuple::make_tuple, Atom, Binary, Decoder, Encoder, Env, NifResult, Resource,
    ResourceArc, Term,
};
use std::sync::Mutex;

mod atoms {
    rustler::atoms! {
        done,
        exclusive,
        forward,
        inclusive,
        reverse,
    }
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

enum Direction {
    Forward,
    Reverse,
}

impl<'a> Decoder<'a> for Direction {
    fn decode(term: Term<'a>) -> NifResult<Direction> {
        let err =
            || rustler::Error::Term(Box::new("expected atom 'forward' | 'reverse'".to_owned()));
        let atom: Atom = term.decode().map_err(|_| err())?;
        if atom == atoms::forward() {
            Ok(Direction::Forward)
        } else if atom == atoms::reverse() {
            Ok(Direction::Reverse)
        } else {
            Err(err())
        }
    }
}

enum Range {
    Inclusive,
    Exclusive,
}

impl<'a> Decoder<'a> for Range {
    fn decode(term: Term<'a>) -> NifResult<Range> {
        let err = || {
            rustler::Error::Term(Box::new(
                "expected atom 'inclusive' | 'exclusive'".to_owned(),
            ))
        };
        let atom: Atom = term.decode().map_err(|_| err())?;
        if atom == atoms::inclusive() {
            Ok(Range::Inclusive)
        } else if atom == atoms::exclusive() {
            Ok(Range::Exclusive)
        } else {
            Err(err())
        }
    }
}

////////////////////////////////////////////////////////////////////////////
// Iterator Creation - Plain Keyspace                                     //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_iter(ks: ResourceArc<KsRsc>, direction: Direction) -> FjallResult<ResourceArc<IterRsc>> {
    let iter = ks.0.iter();
    let inner = match direction {
        Direction::Forward => IterInner::Forward(iter),
        Direction::Reverse => IterInner::Reverse(iter.rev()),
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_range(
    ks: ResourceArc<KsRsc>,
    direction: Direction,
    range: Range,
    start: rustler::Binary,
    end: rustler::Binary,
) -> FjallResult<ResourceArc<IterRsc>> {
    let raw_iter = match range {
        Range::Inclusive => ks.0.range(start.as_slice()..=end.as_slice()),
        Range::Exclusive => ks.0.range(start.as_slice()..end.as_slice()),
    };
    let iter = match direction {
        Direction::Forward => IterInner::Forward(raw_iter),
        Direction::Reverse => IterInner::Reverse(raw_iter.rev()),
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(iter))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn ks_prefix(
    ks: ResourceArc<KsRsc>,
    direction: Direction,
    prefix: rustler::Binary,
) -> FjallResult<ResourceArc<IterRsc>> {
    let iter = ks.0.prefix(prefix.as_slice());
    let inner = match direction {
        Direction::Forward => IterInner::Forward(iter),
        Direction::Reverse => IterInner::Reverse(iter.rev()),
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

////////////////////////////////////////////////////////////////////////////
// Iterator Creation - OTX Keyspace                                       //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_iter(
    ks: ResourceArc<OtxKsRsc>,
    direction: Direction,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let iter = keyspace.iter();
    let inner = match direction {
        Direction::Forward => IterInner::Forward(iter),
        Direction::Reverse => IterInner::Reverse(iter.rev()),
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(inner))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_range(
    ks: ResourceArc<OtxKsRsc>,
    direction: Direction,
    range: Range,
    start: rustler::Binary,
    end: rustler::Binary,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let raw_iter = match range {
        Range::Inclusive => keyspace.range(start.as_slice()..=end.as_slice()),
        Range::Exclusive => keyspace.range(start.as_slice()..end.as_slice()),
    };
    let iter = match direction {
        Direction::Forward => IterInner::Forward(raw_iter),
        Direction::Reverse => IterInner::Reverse(raw_iter.rev()),
    };
    FjallResult(Ok(ResourceArc::new(IterRsc(Mutex::new(Some(iter))))))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn otx_ks_prefix(
    ks: ResourceArc<OtxKsRsc>,
    direction: Direction,
    prefix: rustler::Binary,
) -> FjallResult<ResourceArc<IterRsc>> {
    let keyspace: &fjall::Keyspace = ks.0.as_ref();
    let iter = keyspace.prefix(prefix.as_slice());
    let inner = match direction {
        Direction::Forward => IterInner::Forward(iter),
        Direction::Reverse => IterInner::Reverse(iter.rev()),
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
            atoms::done().to_term(env)
        }
        Some(Ok((k, v))) => {
            let k = make_binary(env, &k).to_term(env);
            let v = make_binary(env, &v).to_term(env);
            let kv = make_tuple(env, &[k, v]);
            make_tuple(env, &[atom::ok().to_term(env), kv])
        }
        Some(Err(e)) => FjallError::from(e).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn iter_collect<'a>(env: Env<'a>, iter: ResourceArc<IterRsc>, limit: usize) -> Term<'a> {
    iter_collect_n(env, iter, Some(limit))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn iter_collect<'a>(env: Env<'a>, iter: ResourceArc<IterRsc>) -> Term<'a> {
    iter_collect_n(env, iter, None)
}

pub fn iter_collect_n<'a>(
    env: Env<'a>,
    iter: ResourceArc<IterRsc>,
    limit: Option<usize>,
) -> Term<'a> {
    let mut guard = iter.0.lock().unwrap();
    let Some(itr) = guard.as_mut() else {
        return (atom::ok(), Vec::<(Binary, Binary)>::new()).encode(env);
    };
    let max = limit.unwrap_or(usize::MAX);
    let mut items: Vec<Term<'a>> = Vec::new();
    for _ in 0..max {
        match itr.next().map(|g| g.into_inner()) {
            Some(Ok((k, v))) => {
                let k = make_binary(env, &k).to_term(env);
                let v = make_binary(env, &v).to_term(env);
                let kv = make_tuple(env, &[k, v]);
                items.push(kv);
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
