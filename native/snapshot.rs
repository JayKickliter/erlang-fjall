use crate::{
    error::{FjallBinaryResult, FjallError, FjallRes},
    otx_ks::OtxKsRsc,
};
use rustler::{Resource, ResourceArc};

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
pub fn snapshot_get(
    snapshot: ResourceArc<SnapshotRsc>,
    ks: ResourceArc<OtxKsRsc>,
    key: rustler::Binary,
) -> FjallBinaryResult {
    let result = (|| {
        use fjall::Readable;
        let val = snapshot
            .snapshot
            .get(&ks.0, key.as_slice())
            .to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}
