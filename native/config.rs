use fjall::Config;
use crate::error::FjallError;

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

pub fn parse_config_options(
    path: String,
    options: Vec<(rustler::Atom, rustler::Term)>,
) -> Result<Config, FjallError> {
    let mut config = Config::new(path);

    for (key, value) in options {
        let key_str = format!("{:?}", key);
        match key_str.as_str() {
            "manual_journal_persist" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                config = config.manual_journal_persist(val);
            }
            "flush_workers" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                config = config.flush_workers(val);
            }
            "compaction_workers" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                config = config.compaction_workers(val);
            }
            "max_open_files" => {
                let val: usize = value.decode().map_err(FjallError::from)?;
                config = config.max_open_files(val);
            }
            "cache_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                config = config.cache_size(val);
            }
            "max_journaling_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                config = config.max_journaling_size(val);
            }
            "max_write_buffer_size" => {
                let val: u64 = value.decode().map_err(FjallError::from)?;
                config = config.max_write_buffer_size(val);
            }
            "fsync_ms" => {
                let val: Option<u16> = value.decode().map_err(FjallError::from)?;
                config = config.fsync_ms(val);
            }
            "temporary" => {
                let val: bool = value.decode().map_err(FjallError::from)?;
                config = config.temporary(val);
            }
            _ => {
                return Err(FjallError::ConfigError(format!(
                    "Unknown config option: {:?}",
                    key
                )))
            }
        }
    }

    Ok(config)
}
