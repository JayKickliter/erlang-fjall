use rustler::{Encoder, Env, Term};

pub mod atom {
    rustler::atoms! {
        batch_already_committed,
        error,
        not_found,
        ok,
        transaction_already_finalized,
        transaction_conflict,
    }
}

////////////////////////////////////////////////////////////////////////////
// Error Handling                                                         //
////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FjallError {
    BatchAlreadyCommitted,
    Config(String),
    Decode(String),
    Fjall(fjall::Error),
    NotFound,
    TransactionAlreadyFinalized,
    TransactionConflict,
    Utf8(std::str::Utf8Error),
}

impl From<fjall::Error> for FjallError {
    fn from(err: fjall::Error) -> Self {
        FjallError::Fjall(err)
    }
}

impl From<fjall::Conflict> for FjallError {
    fn from(_err: fjall::Conflict) -> Self {
        FjallError::TransactionConflict
    }
}

impl From<std::str::Utf8Error> for FjallError {
    fn from(err: std::str::Utf8Error) -> Self {
        FjallError::Utf8(err)
    }
}

impl From<rustler::Error> for FjallError {
    fn from(err: rustler::Error) -> Self {
        FjallError::Decode(format!("{:?}", err))
    }
}

impl Encoder for FjallError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            FjallError::BatchAlreadyCommitted => {
                (atom::error(), atom::batch_already_committed()).encode(env)
            }
            FjallError::Config(msg) => (atom::error(), msg.clone()).encode(env),
            FjallError::Decode(msg) => (atom::error(), msg.clone()).encode(env),
            FjallError::Fjall(e) => {
                let msg = format!("{:?}", e);
                (atom::error(), msg).encode(env)
            }
            FjallError::NotFound => (atom::error(), atom::not_found()).encode(env),
            FjallError::TransactionAlreadyFinalized => {
                (atom::error(), atom::transaction_already_finalized()).encode(env)
            }
            FjallError::TransactionConflict => {
                (atom::error(), atom::transaction_conflict()).encode(env)
            }
            FjallError::Utf8(e) => {
                let msg = format!("UTF-8 error: {}", e);
                (atom::error(), msg).encode(env)
            }
        }
    }
}

impl std::panic::RefUnwindSafe for FjallError {}

////////////////////////////////////////////////////////////////////////////
// Result Wrapper for Erlang Encoding                                    //
////////////////////////////////////////////////////////////////////////////

pub struct FjallResult<T>(pub Result<T, FjallError>);

impl<T: Encoder> Encoder for FjallResult<T> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            Ok(value) => (atom::ok(), value).encode(env),
            Err(err) => err.encode(env),
        }
    }
}

// Wrapper for operations that return just ok or error (not {ok, Value})
pub struct FjallOkResult(pub Result<(), FjallError>);

impl Encoder for FjallOkResult {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            Ok(()) => atom::ok().encode(env),
            Err(err) => err.encode(env),
        }
    }
}

// Wrapper for get operations that return binary data
pub struct FjallBinaryResult(pub Result<Vec<u8>, FjallError>);

impl Encoder for FjallBinaryResult {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            Ok(data) => {
                // Create a binary from the Vec<u8> by encoding it as a binary slice
                match rustler::OwnedBinary::new(data.len()) {
                    Some(mut owned_bin) => {
                        owned_bin.as_mut_slice().copy_from_slice(data);
                        (atom::ok(), owned_bin.release(env)).encode(env)
                    }
                    None => {
                        // Fallback: encode as empty binary if allocation fails
                        (
                            atom::ok(),
                            rustler::OwnedBinary::new(0).unwrap().release(env),
                        )
                            .encode(env)
                    }
                }
            }
            Err(err) => err.encode(env),
        }
    }
}

// Wrapper for operations that return a key-value pair as binaries
pub struct FjallKvResult(pub Result<(Vec<u8>, Vec<u8>), FjallError>);

impl Encoder for FjallKvResult {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            Ok((key, value)) => {
                let key_bin = match rustler::OwnedBinary::new(key.len()) {
                    Some(mut owned_bin) => {
                        owned_bin.as_mut_slice().copy_from_slice(key);
                        owned_bin.release(env)
                    }
                    None => rustler::OwnedBinary::new(0).unwrap().release(env),
                };
                let value_bin = match rustler::OwnedBinary::new(value.len()) {
                    Some(mut owned_bin) => {
                        owned_bin.as_mut_slice().copy_from_slice(value);
                        owned_bin.release(env)
                    }
                    None => rustler::OwnedBinary::new(0).unwrap().release(env),
                };
                (atom::ok(), (key_bin, value_bin)).encode(env)
            }
            Err(err) => err.encode(env),
        }
    }
}

////////////////////////////////////////////////////////////////////////////
// FjallRes Trait                                                         //
////////////////////////////////////////////////////////////////////////////

pub trait FjallRes<T> {
    fn to_erlang_result(self) -> Result<T, FjallError>;
}

impl<T> FjallRes<T> for Result<T, fjall::Error> {
    fn to_erlang_result(self) -> Result<T, FjallError> {
        self.map_err(FjallError::from)
    }
}
