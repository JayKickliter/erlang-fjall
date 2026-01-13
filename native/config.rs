use crate::error::FjallError;
use fjall::DatabaseBuilder;
use std::path::Path;

mod atom {
    rustler::atoms! {
        manual_journal_persist,
        worker_threads,
        max_cached_files,
        cache_size,
        max_journaling_size,
        temporary,
    }
}

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
        if key == atom::manual_journal_persist() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.manual_journal_persist(val);
        } else if key == atom::worker_threads() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.worker_threads(val);
        } else if key == atom::max_cached_files() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.max_cached_files(Some(val));
        } else if key == atom::cache_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.cache_size(val);
        } else if key == atom::max_journaling_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.max_journaling_size(val);
        } else if key == atom::temporary() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.temporary(val);
        } else {
            return Err(FjallError::Config(format!(
                "Unknown config option: {:?}",
                key
            )));
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
        if key == atom::manual_journal_persist() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.manual_journal_persist(val);
        } else if key == atom::worker_threads() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.worker_threads(val);
        } else if key == atom::max_cached_files() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.max_cached_files(Some(val));
        } else if key == atom::cache_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.cache_size(val);
        } else if key == atom::max_journaling_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.max_journaling_size(val);
        } else if key == atom::temporary() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.temporary(val);
        } else {
            return Err(FjallError::Config(format!(
                "Unknown config option: {:?}",
                key
            )));
        }
    }

    Ok(builder)
}
