use crate::{
    error::{FjallError, FjallRes, FjallResult},
    make_binary,
    otx_ks::OtxKsRsc,
};
use rustler::{Encoder, Env, Resource, ResourceArc, Term};

////////////////////////////////////////////////////////////////////////////
// Snapshot Resource                                                      //
////////////////////////////////////////////////////////////////////////////

pub struct SnapshotRsc {
    pub snapshot: fjall::Snapshot,
}

impl std::panic::RefUnwindSafe for SnapshotRsc {}

#[rustler::resource_impl]
impl Resource for SnapshotRsc {}

////////////////////////////////////////////////////////////////////////////
// Snapshot NIFs                                                          //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyIo")]
pub fn snapshot_get<'a>(
    env: Env<'a>,
    snapshot: ResourceArc<SnapshotRsc>,
    ks: ResourceArc<OtxKsRsc>,
    key: rustler::Binary,
) -> FjallResult<Term<'a>> {
    let result = (|| {
        use fjall::Readable;
        let val = snapshot
            .snapshot
            .get(&ks.0, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(make_binary(env, &value).encode(env)),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallResult(result)
}
