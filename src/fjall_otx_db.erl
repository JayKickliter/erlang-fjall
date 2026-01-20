-module(fjall_otx_db).

-moduledoc """
Optimistic transactional database operations.

See [OptimisticTxDatabase](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxDatabase.html)
in the Rust documentation.
""".

-export([
    open/1,
    open/2,
    keyspace/2,
    keyspace/3,
    write_tx/1,
    snapshot/1,
    persist/2
]).

-export_type([otx_db/0]).

-doc """
Opaque handle to an optimistic transactional database instance.
""".
-nominal otx_db() :: reference().

-doc """
Opens a transactional database at the given path with default configuration.

Returns `{ok, Database}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_otx_db:open("/var/lib/txn_db")
```

See [OptimisticTxDatabase::open](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxDatabase.html#method.open)
in the Rust documentation.
""".
-spec open(Path :: file:name_all()) -> fjall:result(otx_db()).
open(Path) ->
    open(Path, []).

-doc """
Opens a transactional database with configuration options.

Accepts the same configuration options as `fjall_db:open/2`. The transactional
database can be used for both transactional and non-transactional
keyspace operations.

Returns `{ok, Database}` on success or `{error, Reason}` on failure.

## Example

```erlang
Options = [
    {cache_size, 512 * 1024 * 1024},
    {worker_threads, 4}
],
{ok, Database} = fjall_otx_db:open("/var/lib/txn_db", Options)
```

See `t:fjall:config_option/0` for available configuration options.
""".
-spec open(Path :: file:name_all(), Options :: [fjall:config_option()]) ->
    fjall:result(otx_db()).
open(Path, Options) ->
    PathBinary = path_to_binary(Path),
    fjall_nif:otx_db_open(PathBinary, Options).

-doc """
Opens or creates a keyspace in a transactional database with default options.

Returns a keyspace handle for use in transactions.

Returns `{ok, Keyspace}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_otx_db:open("./db"),
{ok, Keyspace} = fjall_otx_db:keyspace(Database, <<"users">>)
```

See [OptimisticTxDatabase::keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxDatabase.html#method.keyspace)
in the Rust documentation.

See `m:fjall_otx_tx` and `m:fjall_snapshot` for keyspace operations.
""".
-spec keyspace(Database :: otx_db(), Name :: binary()) ->
    fjall:result(fjall_otx_ks:otx_ks()).
keyspace(Database, Name) ->
    keyspace(Database, Name, []).

-doc """
Opens or creates a keyspace in a transactional database with options.

Returns `{ok, Keyspace}` on success or `{error, Reason}` on failure.
""".
-spec keyspace(Database :: otx_db(), Name :: binary(), Options :: [fjall:ks_option()]) ->
    fjall:result(fjall_otx_ks:otx_ks()).
keyspace(Database, Name, Options) ->
    fjall_nif:otx_db_keyspace(Database, Name, Options).

-doc """
Begins a write transaction on the database.

Returns a transaction handle that can be used for atomic multi-keyspace
writes. The transaction must be explicitly committed with
`fjall_otx_tx:commit/1` or rolled back with `fjall_otx_tx:rollback/1`.

Returns `{ok, Transaction}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace1, <<"key1">>, <<"value1">>),
ok = fjall_otx_tx:insert(Txn, Keyspace2, <<"key2">>, <<"value2">>),
ok = fjall_otx_tx:commit(Txn)
```

See [OptimisticTxDatabase::write_tx](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxDatabase.html#method.write_tx)
in the Rust documentation.

See `m:fjall_otx_tx` for transaction operations.
""".
-spec write_tx(Database :: otx_db()) -> fjall:result(fjall_otx_tx:write_tx()).
write_tx(Database) ->
    fjall_nif:otx_db_write_tx(Database).

-doc """
Begins a read transaction (snapshot) on the database.

Returns a read-only transaction handle that sees a consistent
point-in-time view of the data. Multiple read transactions can run
concurrently.

Returns `{ok, Snapshot}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Snapshot} = fjall_otx_db:snapshot(Database),
{ok, Value} = fjall_snapshot:get(Snapshot, Keyspace, <<"key">>)
```

See [OptimisticTxDatabase::read_tx](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxDatabase.html#method.read_tx)
in the Rust documentation.

See `m:fjall_snapshot` for snapshot operations.
""".
-spec snapshot(Database :: otx_db()) -> fjall:result(fjall_snapshot:snapshot()).
snapshot(Database) ->
    fjall_nif:otx_db_snapshot(Database).

-doc """
Persists the database to disk with the specified durability mode.

This function flushes all in-memory data and journals to storage
according to the specified persist mode.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_otx_db:open("./db"),
ok = fjall_otx_db:persist(Database, sync_all)
```

See `t:fjall:persist_mode/0` for available persist modes.
""".
-spec persist(Database :: otx_db(), Mode :: fjall:persist_mode()) -> fjall:result().
persist(Database, Mode) ->
    fjall_nif:otx_db_persist(Database, Mode).

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
