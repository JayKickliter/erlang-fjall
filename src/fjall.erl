-module(fjall).

-moduledoc """
Fjall embedded key-value database for Erlang.

This module provides a simple interface to the Fjall LSM-tree based
key-value database. All keys and values are binaries.

## Example

```
{ok, Database} = fjall:open("./mydb"),
{ok, Keyspace} = fjall:open_keyspace(Database, <<"default">>),
ok = fjall:insert(Keyspace, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall:get(Keyspace, <<"key">>),
ok = fjall:remove(Keyspace, <<"key">>).
```

## Databases and Keyspaces

A database is a root instance with its own data and
configuration. Within a database, you can create multiple keyspaces,
which are logical separations of key-value data. Each keyspace
maintains its own index and operations are independent.

## Configuration Options

When opening a database, you can pass configuration options to control
behavior like cache size, worker thread counts, and journaling
settings. See `config_option/0` for available options.
""".

-on_load(load/0).

-export([
    open/1,
    open/2,
    open_keyspace/2,
    insert/3,
    get/2,
    remove/2,
    persist/2,
    open_txn/1,
    open_txn/2,
    open_txn_keyspace/2,
    begin_write_txn/1,
    begin_read_txn/1,
    txn_insert/4,
    txn_get/3,
    txn_remove/3,
    read_txn_get/3,
    commit_txn/1,
    rollback_txn/1
]).

-export_type([
    database/0,
    keyspace/0,
    config_option/0,
    persist_mode/0,
    result/0,
    result/1,
    txn_database/0,
    txn_keyspace/0,
    write_txn/0,
    read_txn/0
]).

-doc """
Opaque handle to a Fjall database instance.

Databases are root instances that can contain multiple keyspaces. Use
`open/1` or `open/2` to create or open a database.

See [Database](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html)
in the Rust documentation.
""".
-opaque database() :: reference().

-doc """
Opaque handle to a keyspace within a database.

Keyspaces are logical separations of data within a database.
Use `open_keyspace/2` to open a keyspace for key-value operations.

See [Keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html)
in the Rust documentation.
""".
-opaque keyspace() :: reference().

-doc """
Configuration option for database creation.

Supported options:

- `{cache_size, pos_integer()}` - Total block cache size in bytes.
  Higher values improve read performance but consume more memory.
  Default: 32MB
- `{max_journaling_size, pos_integer()}` - Maximum write-ahead log
  (journal) size in bytes. Older journals are cleaned up as needed.
  Default: 512MB
- `{worker_threads, pos_integer()}` - Number of worker threads for
  background maintenance (flushing and compaction).
  Default: min(CPU cores, 4)
- `{max_cached_files, pos_integer()}` - Maximum number of cached file
  descriptors. Default: 150 (macOS), 900 (Linux), 400 (Windows)
- `{manual_journal_persist, boolean()}` - If `true`, journal
  persistence is manual and must be triggered explicitly.
  Default: `false`
- `{temporary, boolean()}` - If `true`, the database is temporary and
  will be deleted when closed. Default: `false`

See [DatabaseBuilder](https://docs.rs/fjall/3.0.1/fjall/struct.DatabaseBuilder.html)
in the Rust documentation for configuration methods.
""".
-type config_option() ::
    {manual_journal_persist, boolean()}
    | {worker_threads, pos_integer()}
    | {max_cached_files, pos_integer()}
    | {cache_size, pos_integer()}
    | {max_journaling_size, pos_integer()}
    | {temporary, boolean()}.

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

-doc """
Opaque handle to a transactional database instance.

Transactional databases support ACID transactions for atomic
multi-keyspace updates and snapshot-isolated reads.
Use `open_txn/1` or `open_txn/2` to create a transactional database.

See [SingleWriterTxDatabase](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html)
in the Rust documentation.
""".
-opaque txn_database() :: reference().

-doc """
Opaque handle to a keyspace in a transactional database.

Used for operations within transactions. Accessed via
`open_txn_keyspace/2`.

See [Keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html)
in the Rust documentation.
""".
-opaque txn_keyspace() :: reference().

-doc """
Opaque handle to a write transaction.

Provides single-writer serialized transactions with read-your-own-writes
semantics. Created with `begin_write_txn/1`, must be committed with
`commit_txn/1` or rolled back with `rollback_txn/1`.

See [SingleWriterWriteTx](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html)
in the Rust documentation.
""".
-opaque write_txn() :: reference().

-doc """
Opaque handle to a read transaction (snapshot).

Provides snapshot isolation with repeatable read semantics. Created
with `begin_read_txn/1`. Read-only, no explicit commit/rollback needed.

See [Snapshot](https://docs.rs/fjall/3.0.1/fjall/struct.Snapshot.html)
in the Rust documentation.
""".
-opaque read_txn() :: reference().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-doc """
Opens a database at the given path with default configuration options.

The path can be a string, binary, or atom representing a file system
path. If the database already exists at the given path, it will be
opened. Otherwise, a new database will be created.

Returns `{ok, Database}` on success or `{error, Reason}` on failure.

## Errors

Common errors include:

- Permission denied - insufficient permissions to access the path
- Invalid path - path contains invalid UTF-8 sequences
- Disk error - I/O error when accessing the file system

## Example

```erlang
{ok, Database} = fjall:open("/var/lib/mydb")
```

See [Database::open](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.open)
in the Rust documentation.
""".
-spec open(Path :: file:name_all()) -> result(database()).
open(Path) ->
    open(Path, []).

-doc """
Opens a database at the given path with custom configuration options.

Configuration options allow fine-tuning of Fjall's behavior for
specific workloads. Options are provided as a list of tuples. Unknown
options will result in an error.

Returns `{ok, Database}` on success or `{error, Reason}` on failure.

## Example

```erlang
Options = [
    {cache_size, 512 * 1024 * 1024},  % 512MB cache
    {worker_threads, 4}  % 4 worker threads
],
{ok, Database} = fjall:open("/var/lib/mydb", Options)
```

## See Also

- `t:config_option/0` for available configuration options
- [Database::open](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.open)
  in the Rust documentation
""".
-spec open(Path :: file:name_all(), Options :: [config_option()]) ->
    result(database()).
open(Path, Options) ->
    PathBinary = path_to_binary(Path),
    open_nif(PathBinary, Options).

-spec open_nif(Path :: binary(), Options :: [config_option()]) ->
    result(database()).
open_nif(_Path, _Options) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Opens or creates a keyspace within a database.

Keyspaces provide logical separation of data within a single database.
If a keyspace with the given name already exists, it is opened.
Otherwise, a new keyspace is created.

The keyspace name must be a binary. Keyspace names are persistent and
will be recovered when the database is reopened.

Returns `{ok, Keyspace}` on success or `{error, Reason}` on failure.

## Errors

- `{error, disk_error}` - I/O error when accessing keyspace data
- `{error, corrupted}` - Keyspace data is corrupted

## Example

```erlang
{ok, Database} = fjall:open("./db"),
{ok, Keyspace} = fjall:open_keyspace(Database, <<"users">>)
```

See [Database::keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.keyspace)
in the Rust documentation.
""".
-spec open_keyspace(Database :: database(), Name :: binary()) ->
    result(keyspace()).
open_keyspace(_Database, _Name) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Inserts or updates a key-value pair in the keyspace.

If the key already exists, its value is overwritten. Both keys and
values must be binaries.

Returns `ok` on success or `{error, Reason}` on failure.

## Errors

- `{error, disk_error}` - I/O error when writing to disk
- `{error, out_of_memory}` - Insufficient memory to buffer the write

## Example

```erlang
ok = fjall:insert(Keyspace, <<"alice">>, <<"alice@example.com">>)
```

See [Keyspace::insert](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.insert)
in the Rust documentation.
""".
-spec insert(Keyspace :: keyspace(), Key :: binary(), Value :: binary()) ->
    result().
insert(_Keyspace, _Key, _Value) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Retrieves the value associated with a key from the keyspace.

Returns `{ok, Value}` if the key exists, or `{error, not_found}` if
the key does not exist. Returns `{error, Reason}` on other errors.

## Errors

- `{error, not_found}` - Key does not exist in the keyspace
- `{error, disk_error}` - I/O error when reading from disk

## Example

```erlang
case fjall:get(Keyspace, <<"alice">>) of
    {ok, Email} ->
        io:format("Alice's email: ~s~n", [Email]);
    {error, not_found} ->
        io:format("Alice not found~n");
    {error, Reason} ->
        io:format("Error: ~p~n", [Reason])
end
```

See [Keyspace::get](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.get)
in the Rust documentation.
""".
-spec get(Keyspace :: keyspace(), Key :: binary()) -> result(binary()).
get(_Keyspace, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Removes a key-value pair from the keyspace.

If the key does not exist, this is a no-op and still returns `ok`.

Returns `ok` on success or `{error, Reason}` on failure.

## Errors

- `{error, disk_error}` - I/O error when writing to disk

## Example

```erlang
ok = fjall:remove(Keyspace, <<"alice">>)
```

See [Keyspace::remove](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.remove)
in the Rust documentation.
""".
-spec remove(Keyspace :: keyspace(), Key :: binary()) -> result().
remove(_Keyspace, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Persists the database to disk with the specified durability mode.

This function flushes all in-memory data and journals to storage
according to the specified persist mode. It is useful when you want
to ensure all writes are durable, especially when using
`{manual_journal_persist, true}` configuration.

The persist mode controls the durability guarantee:

- `buffer` - Fastest, least durable. Suitable for data that can be
  reconstructed after an application crash.
- `sync_data` - Good balance of performance and durability.
  Recommended for most applications.
- `sync_all` - Slowest, most durable. Ensures both data and metadata
  are written to disk. Recommended for critical data.

Returns `ok` on success or `{error, Reason}` on failure.

## Errors

- `{error, disk_error}` - I/O error when writing to disk

## Example

```erlang
{ok, Database} = fjall:open("./db"),
ok = fjall:insert(Keyspace, <<"key">>, <<"value">>),
ok = fjall:persist(Database, sync_all)
```

See [Database::persist](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.persist)
in the Rust documentation.
""".
-spec persist(Database :: database(), Mode :: persist_mode()) -> result().
persist(_Database, _Mode) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Transactional API                                                      %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-doc """
Opens a transactional database at the given path with default
configuration.

Returns a transactional database that supports ACID transactions for
atomic multi-keyspace writes and snapshot-isolated reads.

See `open/1` for path handling and error information.

See [SingleWriterTxDatabase::open](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html#method.open)
in the Rust documentation.
""".
-spec open_txn(Path :: file:name_all()) -> result(txn_database()).
open_txn(Path) ->
    open_txn(Path, []).

-doc """
Opens a transactional database with configuration options.

Accepts the same configuration options as `open/2`. The transactional
database can be used for both transactional and non-transactional
keyspace operations.

See [SingleWriterTxDatabase::open](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html#method.open)
in the Rust documentation.
""".
-spec open_txn(Path :: file:name_all(), Options :: [config_option()]) ->
    result(txn_database()).
open_txn(Path, Options) ->
    PathBinary = path_to_binary(Path),
    open_txn_nif(PathBinary, Options).

-spec open_txn_nif(Path :: binary(), Options :: [config_option()]) ->
    result(txn_database()).
open_txn_nif(_Path, _Options) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Opens or creates a keyspace in a transactional database.

Returns a keyspace handle for use in transactions. Like
`open_keyspace/2` but for use with transactional databases.

See [SingleWriterTxDatabase::keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html#method.keyspace)
in the Rust documentation.
""".
-spec open_txn_keyspace(Database :: txn_database(), Name :: binary()) ->
    result(txn_keyspace()).
open_txn_keyspace(_Database, _Name) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Begins a write transaction on the database.

Returns a transaction handle that can be used for atomic multi-keyspace
writes. The transaction must be explicitly committed with `commit_txn/1`
or rolled back with `rollback_txn/1`. If neither is called, automatic
rollback occurs when the transaction is garbage collected.

## Semantics

Write transactions provide:

- **Read-your-own-writes (RYOW)**: Reads within the transaction see
  uncommitted writes by the same transaction.
- **Atomicity**: All writes commit or none do.
- **Single-writer serialization**: Transactions are serialized per
  database.
- **Cross-keyspace atomicity**: Can update multiple keyspaces
  atomically.

## Example

```erlang
{ok, Txn} = fjall:begin_write_txn(Database),
ok = fjall:txn_insert(Txn, Keyspace1, <<"key1">>, <<"value1">>),
ok = fjall:txn_insert(Txn, Keyspace2, <<"key2">>, <<"value2">>),
ok = fjall:commit_txn(Txn)  % Both inserts are now atomic
```

See [SingleWriterTxDatabase::write_tx](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html#method.write_tx)
in the Rust documentation.
""".
-spec begin_write_txn(Database :: txn_database()) -> result(write_txn()).
begin_write_txn(_Database) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Begins a read transaction (snapshot) on the database.

Returns a read-only transaction handle that sees a consistent
point-in-time view of the data. Multiple read transactions can run
concurrently.

## Semantics

Read transactions provide:

- **Snapshot isolation**: The transaction sees a consistent
  point-in-time view that remains unchanged for the lifetime of the
  transaction.
- **Repeatable reads**: Reading the same key multiple times returns
  the same value.
- **No dirty reads**: Only sees committed data.
- **Read-only**: No modifications allowed.

## Example

```erlang
{ok, ReadTxn} = fjall:begin_read_txn(Database),
{ok, Value1} = fjall:read_txn_get(ReadTxn, Keyspace, <<"key1">>),
% Even if another write happens now:
% {ok, Value2} = fjall:read_txn_get(ReadTxn, Keyspace, <<"key2">>)
% ReadTxn still sees its original snapshot
```

See [SingleWriterTxDatabase::read_tx](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterTxDatabase.html#method.read_tx)
in the Rust documentation.
""".
-spec begin_read_txn(Database :: txn_database()) -> result(read_txn()).
begin_read_txn(_Database) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Inserts or updates a key-value pair within a write transaction.

If the key already exists, its value is updated. The write is visible
to subsequent reads within the same transaction (read-your-own-writes).

Returns `ok` on success or `{error, Reason}` on failure. The
transaction is not automatically rolled back on error; the caller
must decide whether to commit or rollback.

See [SingleWriterWriteTx::insert](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html#method.insert)
in the Rust documentation.
""".
-spec txn_insert(
    Txn :: write_txn(),
    Keyspace :: txn_keyspace(),
    Key :: binary(),
    Value :: binary()
) -> result().
txn_insert(_Txn, _Keyspace, _Key, _Value) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Retrieves a value from a write transaction.

Returns the value if it exists (including writes made earlier in the
same transaction). Returns `{error, not_found}` if the key doesn't
exist.

## Read-Your-Own-Writes

If the key was inserted or updated earlier in the same transaction,
this returns that value. Otherwise, it returns the value from the
database at transaction start time.

See [SingleWriterWriteTx::get](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html#method.get)
in the Rust documentation.
""".
-spec txn_get(Txn :: write_txn(), Keyspace :: txn_keyspace(), Key :: binary()) ->
    result(binary()).
txn_get(_Txn, _Keyspace, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Removes a key from a write transaction.

If the key doesn't exist, this is a no-op and returns `ok`. The
removal is visible to subsequent reads in the same transaction.

See [SingleWriterWriteTx::remove](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html#method.remove)
in the Rust documentation.
""".
-spec txn_remove(Txn :: write_txn(), Keyspace :: txn_keyspace(), Key :: binary()) ->
    result().
txn_remove(_Txn, _Keyspace, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Retrieves a value from a read transaction (snapshot).

Returns the value if it exists in the snapshot, or `{error, not_found}`
if it doesn't. All reads in the same read transaction see the same
snapshot, regardless of concurrent writes.

See [Snapshot::get](https://docs.rs/fjall/3.0.1/fjall/struct.Snapshot.html#method.get)
in the Rust documentation.
""".
-spec read_txn_get(Txn :: read_txn(), Keyspace :: txn_keyspace(), Key :: binary()) ->
    result(binary()).
read_txn_get(_Txn, _Keyspace, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Commits a write transaction, making all changes durable.

Atomically applies all changes made in the transaction. On success,
returns `ok`. On failure, returns an error and the transaction is
rolled back.

After commit, the transaction handle is invalid and cannot be used
for further operations (will return
`{error, transaction_already_finalized}`).

## Atomicity Guarantee

Either all writes in the transaction are applied, or none are. There
is no middle ground.

See [SingleWriterWriteTx::commit](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html#method.commit)
in the Rust documentation.
""".
-spec commit_txn(Txn :: write_txn()) -> result().
commit_txn(_Txn) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-doc """
Rolls back a write transaction, discarding all changes.

After rollback, the transaction handle is invalid and cannot be used
for further operations.

## Note

Rollback is optional. If a transaction is dropped without being
committed or explicitly rolled back, automatic rollback occurs.

See [SingleWriterWriteTx::rollback](https://docs.rs/fjall/3.0.1/fjall/struct.SingleWriterWriteTx.html#method.rollback)
in the Rust documentation.
""".
-spec rollback_txn(Txn :: write_txn()) -> result().
rollback_txn(_Txn) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private Helper Functions                                               %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec path_to_binary(Path :: file:name_all()) -> binary().
path_to_binary(Path) when is_binary(Path) ->
    Path;
path_to_binary(Path) when is_list(Path) ->
    case file:native_name_encoding() of
        utf8 ->
            unicode:characters_to_binary(Path, unicode, utf8);
        latin1 ->
            unicode:characters_to_binary(Path, latin1, utf8)
    end;
path_to_binary(Path) when is_atom(Path) ->
    atom_to_binary(Path, utf8).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF loader                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load() ->
    SoName =
        case code:priv_dir(?MODULE) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, libnative]);
                    _ ->
                        filename:join([priv, libnative])
                end;
            Dir ->
                filename:join(Dir, libnative)
        end,
    case erlang:load_nif(SoName, 0) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
