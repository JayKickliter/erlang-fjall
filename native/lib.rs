mod config;
mod db;
mod error;
mod iter;
mod ks;
mod otx_db;
mod otx_ks;
mod otx_tx;
mod snapshot;
mod wb;

use rustler::{Binary, Env, NewBinary};

pub fn make_binary<'a>(env: Env<'a>, data: &[u8]) -> Binary<'a> {
    let mut bin = NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}

rustler::init!("fjall_nif");
