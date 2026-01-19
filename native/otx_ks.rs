use rustler::Resource;

////////////////////////////////////////////////////////////////////////////
// Optimistic Transaction Keyspace Resource                              //
////////////////////////////////////////////////////////////////////////////

pub struct OtxKsRsc(pub fjall::OptimisticTxKeyspace);

impl std::panic::RefUnwindSafe for OtxKsRsc {}

#[rustler::resource_impl]
impl Resource for OtxKsRsc {}
