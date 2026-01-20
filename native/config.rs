use crate::error::FjallError;
use fjall::DatabaseBuilder;
use std::path::Path;

mod atom {
    rustler::atoms! {
        cache_size,
        expect_point_read_hits,
        journal_compression,
        lz4,
        manual_journal_persist,
        max_cached_files,
        max_journaling_size,
        max_memtable_size,
        none,
        temporary,
        worker_threads,
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

pub fn parse_db_options(
    path: &str,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<DatabaseBuilder<fjall::Database>, FjallError> {
    let mut builder = fjall::Database::builder(Path::new(path));

    for (key, value) in options {
        if key == atom::cache_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.cache_size(val);
        } else if key == atom::journal_compression() {
            let val: rustler::Atom = value.decode().map_err(FjallError::from)?;
            let compression = if val == atom::lz4() {
                fjall::CompressionType::Lz4
            } else if val == atom::none() {
                fjall::CompressionType::None
            } else {
                return Err(FjallError::Config(format!(
                    "Unknown compression type: {:?}",
                    val
                )));
            };
            builder = builder.journal_compression(compression);
        } else if key == atom::manual_journal_persist() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.manual_journal_persist(val);
        } else if key == atom::max_cached_files() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.max_cached_files(Some(val));
        } else if key == atom::max_journaling_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.max_journaling_size(val);
        } else if key == atom::temporary() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.temporary(val);
        } else if key == atom::worker_threads() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.worker_threads(val);
        } else {
            return Err(FjallError::Config(format!(
                "Unknown config option: {:?}",
                key
            )));
        }
    }

    Ok(builder)
}

pub fn parse_otx_db_options(
    path: &str,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<DatabaseBuilder<fjall::OptimisticTxDatabase>, FjallError> {
    let mut builder = fjall::OptimisticTxDatabase::builder(Path::new(path));

    for (key, value) in options {
        if key == atom::cache_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.cache_size(val);
        } else if key == atom::journal_compression() {
            let val: rustler::Atom = value.decode().map_err(FjallError::from)?;
            let compression = if val == atom::lz4() {
                fjall::CompressionType::Lz4
            } else if val == atom::none() {
                fjall::CompressionType::None
            } else {
                return Err(FjallError::Config(format!(
                    "Unknown compression type: {:?}",
                    val
                )));
            };
            builder = builder.journal_compression(compression);
        } else if key == atom::manual_journal_persist() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.manual_journal_persist(val);
        } else if key == atom::max_cached_files() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.max_cached_files(Some(val));
        } else if key == atom::max_journaling_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            builder = builder.max_journaling_size(val);
        } else if key == atom::temporary() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            builder = builder.temporary(val);
        } else if key == atom::worker_threads() {
            let val: usize = value.decode().map_err(FjallError::from)?;
            builder = builder.worker_threads(val);
        } else {
            return Err(FjallError::Config(format!(
                "Unknown config option: {:?}",
                key
            )));
        }
    }

    Ok(builder)
}

////////////////////////////////////////////////////////////////////////////
// Keyspace Configuration Parsing                                         //
////////////////////////////////////////////////////////////////////////////

pub fn parse_ks_options(
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<fjall::KeyspaceCreateOptions, FjallError> {
    let mut ks_options = fjall::KeyspaceCreateOptions::default();

    for (key, value) in options {
        if key == atom::manual_journal_persist() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            ks_options = ks_options.manual_journal_persist(val);
        } else if key == atom::max_memtable_size() {
            let val: u64 = value.decode().map_err(FjallError::from)?;
            ks_options = ks_options.max_memtable_size(val);
        } else if key == atom::expect_point_read_hits() {
            let val: bool = value.decode().map_err(FjallError::from)?;
            ks_options = ks_options.expect_point_read_hits(val);
        } else {
            return Err(FjallError::Config(format!(
                "Unknown keyspace option: {:?}",
                key
            )));
        }
    }

    Ok(ks_options)
}
