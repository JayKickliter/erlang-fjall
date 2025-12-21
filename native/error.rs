use rustler::{Encoder, Env, Term};

pub mod atom {
    rustler::atoms! {
        ok,
        error,
        not_found,
    }
}

////////////////////////////////////////////////////////////////////////////
// Error Handling                                                         //
////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FjallError {
    Fjall(fjall::Error),
    Config(String),
    NotFound,
    Utf8(std::str::Utf8Error),
    Decode(String),
}

impl From<fjall::Error> for FjallError {
    fn from(err: fjall::Error) -> Self {
        FjallError::Fjall(err)
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

impl std::panic::RefUnwindSafe for FjallError {}

impl Encoder for FjallError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            FjallError::Fjall(e) => {
                let msg = format!("{:?}", e);
                (atom::error(), msg).encode(env)
            }
            FjallError::Config(msg) => (atom::error(), msg.clone()).encode(env),
            FjallError::NotFound => (atom::error(), atom::not_found()).encode(env),
            FjallError::Utf8(e) => {
                let msg = format!("UTF-8 error: {}", e);
                (atom::error(), msg).encode(env)
            }
            FjallError::Decode(msg) => (atom::error(), msg.clone()).encode(env),
        }
    }
}

////////////////////////////////////////////////////////////////////////////
// Result Wrapper for Erlang Encoding                                    //
////////////////////////////////////////////////////////////////////////////

pub struct FjallResult<T>(pub Result<T, FjallError>);

impl<T: std::panic::RefUnwindSafe> std::panic::RefUnwindSafe for FjallResult<T> {}

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

impl std::panic::RefUnwindSafe for FjallOkResult {}

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

impl std::panic::RefUnwindSafe for FjallBinaryResult {}

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
