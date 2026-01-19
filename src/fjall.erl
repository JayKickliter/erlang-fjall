-module(fjall).

-moduledoc """
Fjall embedded key-value database for Erlang.

This module provides shared types and configuration options used across
the Fjall database modules.

## Modules

- `fjall_db` - Plain non-transactional database operations
- `fjall_ks` - Plain non-transactional keyspace operations
- `fjall_wb` - Write batch operations
- `fjall_otx_db` - Optimistic transactional database operations
- `fjall_otx_ks` - Optimistic transactional keyspace (type-only)
- `fjall_otx_tx` - Write transaction operations
- `fjall_snapshot` - Read snapshot operations

## Example (Non-Transactional)

```erlang
{ok, Database} = fjall_db:open("./mydb"),
{ok, Keyspace} = fjall_db:keyspace(Database, <<"default">>),
ok = fjall_ks:insert(Keyspace, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall_ks:get(Keyspace, <<"key">>),
ok = fjall_ks:remove(Keyspace, <<"key">>).
```

## Example (Transactional)

```erlang
{ok, Database} = fjall_otx_db:open("./txn_db"),
{ok, Keyspace} = fjall_otx_db:keyspace(Database, <<"default">>),
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall_otx_tx:get(Txn, Keyspace, <<"key">>),
ok = fjall_otx_tx:commit(Txn).
```
""".

-on_load(load/0).

% Export NIF functions (will be called by other modules)
-export([
    % fjall_db NIFs
    db_open_nif/2,
    db_keyspace/3,
    db_batch/1,
    db_persist/2,
    % fjall_ks NIFs
    ks_get/2,
    ks_insert/3,
    ks_remove/2,
    ks_disk_space/1,
    % fjall_wb NIFs
    wb_insert/4,
    wb_remove/3,
    wb_commit/1,
    wb_commit_with_mode/2,
    wb_len/1,
    wb_is_empty/1,
    % fjall_otx_db NIFs
    otx_db_open_nif/2,
    otx_db_keyspace/3,
    otx_db_write_tx/1,
    otx_db_snapshot/1,
    otx_db_persist/2,
    % fjall_otx_tx NIFs
    otx_tx_insert/4,
    otx_tx_get/3,
    otx_tx_remove/3,
    otx_tx_commit/1,
    otx_tx_rollback/1,
    % fjall_snapshot NIFs
    snapshot_get/3
]).

-export_type([
    compression/0,
    config_option/0,
    persist_mode/0,
    result/0,
    result/1
]).

-doc """
Compression type for journal entries.

See [CompressionType](https://docs.rs/fjall/3.0.1/fjall/enum.CompressionType.html)
in the Rust documentation.
""".
-type compression() :: lz4 | none.

-doc """
Configuration option for database creation.

Supported options:

- `{cache_size, pos_integer()}` - Total block cache size in bytes.
  Higher values improve read performance but consume more memory.
  Default: 32MB
- `{journal_compression, compression()}` - Compression type for large
  values written to the journal. See `t:compression/0`. Default: `lz4`
- `{manual_journal_persist, boolean()}` - If `true`, journal
  persistence is manual and must be triggered explicitly.
  Default: `false`
- `{max_cached_files, pos_integer()}` - Maximum number of cached file
  descriptors. Default: 150 (macOS), 900 (Linux), 400 (Windows)
- `{max_journaling_size, pos_integer()}` - Maximum write-ahead log
  (journal) size in bytes. Older journals are cleaned up as needed.
  Default: 512MB
- `{temporary, boolean()}` - If `true`, the database is temporary and
  will be deleted when closed. Default: `false`
- `{worker_threads, pos_integer()}` - Number of worker threads for
  background maintenance (flushing and compaction).
  Default: min(CPU cores, 4)

See [DatabaseBuilder](https://docs.rs/fjall/3.0.1/fjall/struct.DatabaseBuilder.html)
in the Rust documentation for configuration methods.
""".
-type config_option() ::
    {cache_size, pos_integer()}
    | {journal_compression, compression()}
    | {manual_journal_persist, boolean()}
    | {max_cached_files, pos_integer()}
    | {max_journaling_size, pos_integer()}
    | {temporary, boolean()}
    | {worker_threads, pos_integer()}.

-doc """
Persist mode for database persistence.

Determines the durability guarantee when persisting a database:

- `buffer` - Flush to OS buffers only. Data survives application crash
  but not power loss or OS crash. See
  [PersistMode::Buffer](https://docs.rs/fjall/3.0.1/fjall/enum.PersistMode.html#variant.Buffer).
- `sync_data` - Flush with fdatasync. Ensures data is written to disk,
  suitable for most file systems. See
  [PersistMode::SyncData](https://docs.rs/fjall/3.0.1/fjall/enum.PersistMode.html#variant.SyncData).
- `sync_all` - Flush with fsync. Strongest guarantee, ensuring both
  data and metadata are written to disk. See
  [PersistMode::SyncAll](https://docs.rs/fjall/3.0.1/fjall/enum.PersistMode.html#variant.SyncAll).

See [PersistMode](https://docs.rs/fjall/3.0.1/fjall/enum.PersistMode.html)
in the Rust documentation.
""".
-type persist_mode() ::
    buffer
    | sync_data
    | sync_all.

-doc """
Result type for operations that don't return a value on success.
""".
-type result() :: ok | {error, Reason :: term()}.

-doc """
Result type for operations that return a value on success.
""".
-type result(T) :: {ok, T} | {error, Reason :: term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF Stubs                                                              %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% fjall_db NIFs
db_open_nif(_Path, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
db_keyspace(_Db, _Name, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
db_batch(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
db_persist(_Db, _Mode) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_ks NIFs
ks_get(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
ks_insert(_Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
ks_remove(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
ks_disk_space(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_wb NIFs
wb_insert(_Batch, _Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
wb_remove(_Batch, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
wb_commit(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
wb_commit_with_mode(_Batch, _Mode) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
wb_len(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
wb_is_empty(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_otx_db NIFs
otx_db_open_nif(_Path, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_db_keyspace(_Db, _Name, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_db_write_tx(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_db_snapshot(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_db_persist(_Db, _Mode) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_otx_tx NIFs
otx_tx_insert(_Tx, _Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_tx_get(_Tx, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_tx_remove(_Tx, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_tx_commit(_Tx) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
otx_tx_rollback(_Tx) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_snapshot NIFs
snapshot_get(_Snapshot, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF loader                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load() ->
    SoName =
        case code:priv_dir(?MODULE) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, "libfjall-native"]);
                    _ ->
                        filename:join([priv, "libfjall-native"])
                end;
            Dir ->
                filename:join(Dir, "libfjall-native")
        end,
    erlang:load_nif(SoName, 0).
