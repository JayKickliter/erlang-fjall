-module(fjall_db).

-moduledoc """
Plain non-transactional database operations.

This module provides operations for managing a plain Fjall database without
transaction support. For transactional databases, use `fjall_otx_db` instead.

See [Database](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html)
in the Rust documentation.
""".

-export([
    open/1,
    open/2,
    keyspace/2,
    keyspace/3,
    batch/1,
    persist/2
]).

-export_type([db/0]).

-doc """
Opaque handle to a plain Fjall database instance.

Databases are root instances that can contain multiple keyspaces. Use
`open/1` or `open/2` to create or open a database.
""".
-nominal db() :: reference().

-doc """
Opens a database at the given path with default configuration options.

The path can be a string, binary, or atom representing a file system
path. If the database already exists at the given path, it will be
opened. Otherwise, a new database will be created.

Returns `{ok, Database}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_db:open("/var/lib/mydb")
```

See [Database::open](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.open)
in the Rust documentation.
""".
-spec open(Path :: file:name_all()) -> fjall:result(db()).
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
{ok, Database} = fjall_db:open("/var/lib/mydb", Options)
```

See `t:fjall:config_option/0` for available configuration options.
""".
-spec open(Path :: file:name_all(), Options :: [fjall:config_option()]) ->
    fjall:result(db()).
open(Path, Options) ->
    PathBinary = path_to_binary(Path),
    open_nif(PathBinary, Options).

-spec open_nif(Path :: binary(), Options :: [fjall:config_option()]) ->
    fjall:result(db()).
open_nif(Path, Options) ->
    fjall_nif:db_open(Path, Options).

-doc """
Opens or creates a keyspace within a database with default options.

Keyspaces provide logical separation of data within a single database.
If a keyspace with the given name already exists, it is opened.
Otherwise, a new keyspace is created.

Returns `{ok, Keyspace}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_db:open("./db"),
{ok, Keyspace} = fjall_db:keyspace(Database, <<"users">>)
```

See [Database::keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.keyspace)
in the Rust documentation.

See `m:fjall_ks` for keyspace operations.
""".
-spec keyspace(Database :: db(), Name :: binary()) ->
    fjall:result(fjall_ks:ks()).
keyspace(Database, Name) ->
    keyspace(Database, Name, []).

-doc """
Opens or creates a keyspace within a database with configuration options.

Returns `{ok, Keyspace}` on success or `{error, Reason}` on failure.
""".
-spec keyspace(Database :: db(), Name :: binary(), Options :: [fjall:ks_option()]) ->
    fjall:result(fjall_ks:ks()).
keyspace(Database, Name, Options) ->
    fjall_nif:db_keyspace(Database, Name, Options).

-doc """
Creates a new write batch for atomic multi-keyspace writes.

Write batches allow grouping multiple write operations across different
keyspaces into a single atomic operation.

Returns `{ok, WriteBatch}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_db:open("./db"),
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:insert(Batch, Keyspace1, <<"key1">>, <<"value1">>),
ok = fjall_wb:insert(Batch, Keyspace2, <<"key2">>, <<"value2">>),
ok = fjall_wb:commit(Batch)
```

See [Database::batch](https://docs.rs/fjall/3.0.1/fjall/struct.Database.html#method.batch)
in the Rust documentation.

See `m:fjall_wb` for write batch operations.
""".
-spec batch(Database :: db()) -> fjall:result(fjall_wb:wb()).
batch(Database) ->
    fjall_nif:db_batch(Database).

-doc """
Persists the database to disk with the specified durability mode.

This function flushes all in-memory data and journals to storage
according to the specified persist mode.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Database} = fjall_db:open("./db"),
ok = fjall_db:persist(Database, sync_all)
```

See `t:fjall:persist_mode/0` for available persist modes.
""".
-spec persist(Database :: db(), Mode :: fjall:persist_mode()) -> fjall:result().
persist(Database, Mode) ->
    fjall_nif:db_persist(Database, Mode).

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
