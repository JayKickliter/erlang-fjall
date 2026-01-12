use crate::error::FjallError;
use fjall::DatabaseBuilder;
use std::path::Path;

////////////////////////////////////////////////////////////////////////////
// Path Decoding                                                          //
////////////////////////////////////////////////////////////////////////////

pub fn decode_path(bin: rustler::Binary) -> Result<String, FjallError> {
    std::str::from_utf8(bin.as_slice())
        .map(|s| s.to_string())
        .map_err(FjallError::from)
}

////////////////////////////////////////////////////////////////////////////
// Configuration Parsing                                                  //
////////////////////////////////////////////////////////////////////////////

pub fn parse_builder_options_database(
    path: &str,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<DatabaseBuilder<fjall::Database>, FjallError> {
    let mut builder = fjall::Database::builder(Path::new(path));

    for (key, value) in options {
        let key_str = format!("{:?}", key);
        match key_str.as_str() {
            "manual_journal_persist" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                builder = builder.manual_journal_persist(val);
            }
            "worker_threads" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                builder = builder.worker_threads(val);
            }
            "max_cached_files" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                builder = builder.max_cached_files(Some(val));
            }
            "cache_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                builder = builder.cache_size(val);
            }
            "max_journaling_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                builder = builder.max_journaling_size(val);
            }
            "temporary" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                builder = builder.temporary(val);
            }
            _ => {
                return Err(FjallError::Config(format!(
                    "Unknown config option: {:?}",
                    key
                )))
            }
        }
    }

    Ok(builder)
}

pub fn parse_builder_options_optimistic_tx(
    path: &str,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<DatabaseBuilder<fjall::OptimisticTxDatabase>, FjallError> {
    let mut builder = fjall::OptimisticTxDatabase::builder(Path::new(path));

    for (key, value) in options {
        let key_str = format!("{:?}", key);
        match key_str.as_str() {
            "manual_journal_persist" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                builder = builder.manual_journal_persist(val);
            }
            "worker_threads" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                builder = builder.worker_threads(val);
            }
            "max_cached_files" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                builder = builder.max_cached_files(Some(val));
            }
            "cache_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                builder = builder.cache_size(val);
            }
            "max_journaling_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                builder = builder.max_journaling_size(val);
            }
            "temporary" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                builder = builder.temporary(val);
            }
            _ => {
                return Err(FjallError::Config(format!(
                    "Unknown config option: {:?}",
                    key
                )))
            }
        }
    }

    Ok(builder)
}
