use crate::error::{FjallBinaryResult, FjallOkResult, FjallResult, FjallError, FjallRes};
use crate::keyspace::KeyspaceRsc;
use rustler::ResourceArc;

pub use crate::keyspace::PartitionRsc;

////////////////////////////////////////////////////////////////////////////
// Partition NIFs                                                         //
////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
pub fn open_partition(
    keyspace: ResourceArc<KeyspaceRsc>,
    name: String,
) -> FjallResult<ResourceArc<PartitionRsc>> {
    let result = (|| {
        let partition = keyspace
            .0
            .open_partition(&name, Default::default())
            .to_erlang_result()?;
        Ok(ResourceArc::new(PartitionRsc(partition)))
    })();
    FjallResult(result)
}

#[rustler::nif]
pub fn insert(
    partition: ResourceArc<PartitionRsc>,
    key: rustler::Binary,
    value: rustler::Binary,
) -> FjallOkResult {
    let result = (|| {
        partition
            .0
            .insert(key.as_slice(), value.as_slice())
            .to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}

#[rustler::nif]
pub fn get(partition: ResourceArc<PartitionRsc>, key: rustler::Binary) -> FjallBinaryResult {
    let result = (|| {
        let val = partition.0.get(key.as_slice()).to_erlang_result()?;
        match val {
            Some(value) => Ok(value.to_vec()),
            None => Err(FjallError::NotFound),
        }
    })();
    FjallBinaryResult(result)
}

#[rustler::nif]
pub fn remove(
    partition: ResourceArc<PartitionRsc>,
    key: rustler::Binary,
) -> FjallOkResult {
    let result = (|| {
        partition.0.remove(key.as_slice()).to_erlang_result()?;
        Ok(())
    })();
    FjallOkResult(result)
}
