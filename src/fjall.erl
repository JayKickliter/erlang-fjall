-module(fjall).

-moduledoc """
Fjall embedded key-value database for Erlang.

This module provides a unified API that dispatches on tuple tags.

## Examples

### Non-transactional
```erlang
{ok, Db} = fjall:open("./mydb"),
{ok, Ks} = fjall:keyspace(Db, <<"default">>),
ok = fjall:insert(Ks, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall:get(Ks, <<"key">>),
ok = fjall:remove(Ks, <<"key">>).
```

### Optimistic Transaction

```erlang
{ok, Db} = fjall:open("./mydb", [{optimistic, true}]),
{ok, Ks} = fjall:keyspace(Db, <<"default">>),
{ok, Tx} = fjall:write_tx(Db),
ok = fjall:insert(Tx, Ks, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall:get(Tx, Ks, <<"key">>),
ok = fjall:commit(Tx).
```
""".

-export_type([
    batch/0,
    compression/0,
    config_option/0,
    db/0,
    direction/0,
    iter/0,
    ks/0,
    ks_option/0,
    persist_mode/0,
    range/0,
    result/0,
    result/1,
    snapshot/0,
    tx/0
]).

%% Flattened API - dispatch on tuple tags
-export([
    %% Database
    open/1, open/2,
    keyspace/2, keyspace/3,
    batch/1,
    write_tx/1,
    snapshot/1,
    persist/2,

    %% Key-value operations
    get/2, get/3,
    insert/3, insert/4,
    remove/2, remove/3,

    %% Batch/transaction
    commit/1,
    rollback/1,
    len/1,
    is_empty/1,

    %% Keyspace info
    take/2,
    contains_key/2,
    size_of/2,
    disk_space/1,
    approximate_len/1,
    first_key_value/1,
    last_key_value/1,
    path/1,

    %% Iterators
    iter/2,
    iter/3,
    iter/4,
    next/1,
    collect/1,
    collect/2,
    destroy/1
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
Iterator direction.

Specifies the order in which key-value pairs are returned:

- `forward` - Iterate from first to last (ascending key order)
- `reverse` - Iterate from last to first (descending key order)
""".
-type direction() :: forward | reverse.

-doc """
Range boundary type.

Specifies whether the end boundary of a range is included:

- `inclusive` - End boundary is included in the range `[Start, End]`
- `exclusive` - End boundary is excluded from the range `[Start, End)`
""".
-type range() :: inclusive | exclusive.

-doc """
Keyspace configuration option.

Supported options:

- `{manual_journal_persist, boolean()}` - If `true`, journal
  persistence is manual and must be triggered explicitly.
  Default: `false`
- `{max_memtable_size, pos_integer()}` - Maximum in-memory buffer
  size in bytes. Recommended range: 8-64 MiB. Default: 64 MiB
- `{expect_point_read_hits, boolean()}` - If `true`, disables
  last-level bloom filters for ~90% size reduction. Use when
  point reads typically succeed. Default: `false`

See [KeyspaceCreateOptions](https://docs.rs/fjall/latest/fjall/struct.KeyspaceCreateOptions.html)
in the Rust documentation.
""".
-type ks_option() ::
    {manual_journal_persist, boolean()}
    | {max_memtable_size, pos_integer()}
    | {expect_point_read_hits, boolean()}.

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
Result type for operations that return a value on success, an error,
or a sentinal.
""".
-type result(T, S) :: {ok, T} | {error, Reason :: term()} | S.

-doc "Opaque handle to a database (plain or optimistic transactional).".
-opaque db() :: {db, fjall_db:db()} | {otx_db, fjall_otx_db:otx_db()}.

-doc "Opaque handle to a keyspace (plain or optimistic transactional).".
-opaque ks() :: {ks, fjall_ks:ks()} | {otx_ks, fjall_otx_ks:otx_ks()}.

-doc "Opaque handle to a write batch.".
-opaque batch() :: {batch, fjall_wb:wb()}.

-doc "Opaque handle to a write transaction.".
-opaque tx() :: {tx, fjall_otx_tx:write_tx()}.

-doc "Opaque handle to a database snapshot.".
-opaque snapshot() :: {snapshot, fjall_snapshot:snapshot()}.

-doc "Opaque handle to an iterator.".
-opaque iter() :: fjall_iter:iter().

%%--------------------------------------------------------------------------
%% Database
%%--------------------------------------------------------------------------

-doc "Opens a database at `Path` with default options.".
-spec open(Path :: file:name_all()) -> result(db()).
open(Path) ->
    open(Path, []).

-doc "Opens a database at `Path` with `Options`.".
-spec open(Path :: file:name_all(), Options :: [config_option() | {optimistic, boolean()}]) ->
    result(db()).
open(Path, Options) ->
    case proplists:get_value(optimistic, Options, false) of
        true ->
            OptsWithoutTx = proplists:delete(optimistic, Options),
            wrap_otx_db(fjall_otx_db:open(Path, OptsWithoutTx));
        false ->
            OptsWithoutTx = proplists:delete(optimistic, Options),
            case fjall_db:open(Path, OptsWithoutTx) of
                {ok, Ref} -> {ok, {db, Ref}};
                Err -> Err
            end
    end.

-doc "Creates a write batch for atomic multi-keyspace writes.".
-spec batch(db()) -> result(batch()).
batch({db, Ref}) ->
    case fjall_db:batch(Ref) of
        {ok, BatchRef} -> {ok, {batch, BatchRef}};
        Err -> Err
    end.

-doc "Persists the database journal with the given `Mode`.".
-spec persist(db(), Mode :: persist_mode()) -> result().
persist({db, Ref}, Mode) ->
    fjall_db:persist(Ref, Mode);
persist({otx_db, Ref}, Mode) ->
    fjall_otx_db:persist(Ref, Mode).

-doc "Opens or creates a keyspace with `Name`.".
-spec keyspace(db(), Name :: binary()) -> result(ks()).
keyspace({db, Ref}, Name) ->
    wrap_ks(fjall_db:keyspace(Ref, Name));
keyspace({otx_db, Ref}, Name) ->
    wrap_otx_ks(fjall_otx_db:keyspace(Ref, Name)).

-doc "Opens or creates a keyspace with `Name` and `Opts`.".
-spec keyspace(db(), Name :: binary(), Opts :: [ks_option()]) -> result(ks()).
keyspace({db, Ref}, Name, Opts) ->
    wrap_ks(fjall_db:keyspace(Ref, Name, Opts));
keyspace({otx_db, Ref}, Name, Opts) ->
    wrap_otx_ks(fjall_otx_db:keyspace(Ref, Name, Opts)).

-doc "Creates an optimistic write transaction.".
-spec write_tx(db()) -> result(tx()).
write_tx({otx_db, Ref}) ->
    wrap_tx(fjall_otx_db:write_tx(Ref)).

-doc "Creates a point-in-time snapshot for consistent reads.".
-spec snapshot(db()) -> result(snapshot()).
snapshot({otx_db, Ref}) ->
    wrap_snapshot(fjall_otx_db:snapshot(Ref)).

%%--------------------------------------------------------------------------
%% Key-Value Operations
%%--------------------------------------------------------------------------

-doc "Returns the value for `Key`, or `not_found` if it doesn't exist.".
-spec get(ks(), Key :: binary()) -> result(binary(), not_found).
get({ks, Ref}, Key) ->
    fjall_ks:get(Ref, Key);
get({otx_ks, Ref}, Key) ->
    fjall_otx_ks:get(Ref, Key).

-doc "Returns the value for `Key` within a transaction or snapshot.".
-spec get
    (tx(), ks(), Key :: binary()) -> result(binary());
    (snapshot(), ks(), Key :: binary()) -> result(binary(), not_found).
get({tx, TxRef}, {otx_ks, KsRef}, Key) ->
    fjall_otx_tx:get(TxRef, KsRef, Key);
get({snapshot, SnapRef}, {otx_ks, KsRef}, Key) ->
    fjall_snapshot:get(SnapRef, KsRef, Key).

-doc "Inserts or updates `Key` with `Value`.".
-spec insert(ks(), Key :: binary(), Value :: binary()) -> result().
insert({ks, Ref}, Key, Value) ->
    fjall_ks:insert(Ref, Key, Value);
insert({otx_ks, Ref}, Key, Value) ->
    fjall_otx_ks:insert(Ref, Key, Value).

-doc "Inserts or updates `Key` with `Value` within a batch or transaction.".
-spec insert
    (batch(), ks(), Key :: binary(), Value :: binary()) -> result();
    (tx(), ks(), Key :: binary(), Value :: binary()) -> result().
insert({batch, BRef}, {ks, KsRef}, Key, Value) ->
    fjall_wb:insert(BRef, KsRef, Key, Value);
insert({tx, TxRef}, {otx_ks, KsRef}, Key, Value) ->
    fjall_otx_tx:insert(TxRef, KsRef, Key, Value).

-doc "Removes `Key` from the keyspace.".
-spec remove(ks(), Key :: binary()) -> result().
remove({ks, Ref}, Key) ->
    fjall_ks:remove(Ref, Key);
remove({otx_ks, Ref}, Key) ->
    fjall_otx_ks:remove(Ref, Key).

-doc "Removes `Key` from the keyspace within a batch or transaction.".
-spec remove
    (batch(), ks(), Key :: binary()) -> result();
    (tx(), ks(), Key :: binary()) -> result().
remove({batch, BRef}, {ks, KsRef}, Key) ->
    fjall_wb:remove(BRef, KsRef, Key);
remove({tx, TxRef}, {otx_ks, KsRef}, Key) ->
    fjall_otx_tx:remove(TxRef, KsRef, Key).

-doc "Returns the approximate disk space used by the keyspace in bytes.".
-spec disk_space(ks()) -> non_neg_integer().
disk_space({ks, Ref}) ->
    fjall_ks:disk_space(Ref).

%%--------------------------------------------------------------------------
%% Batch/Transaction
%%--------------------------------------------------------------------------

-doc "Commits a batch or transaction.".
-spec commit(batch() | tx()) -> result().
commit({batch, Ref}) ->
    fjall_wb:commit(Ref);
commit({tx, Ref}) ->
    fjall_otx_tx:commit(Ref).

-doc "Returns the number of operations in the batch.".
-spec len(batch()) -> non_neg_integer().
len({batch, Ref}) ->
    fjall_wb:len(Ref).

-doc "Returns `true` if the batch contains no operations.".
-spec is_empty(batch()) -> boolean().
is_empty({batch, Ref}) ->
    fjall_wb:is_empty(Ref).

-doc "Rolls back a transaction, discarding all changes.".
-spec rollback(tx()) -> result().
rollback({tx, Ref}) ->
    fjall_otx_tx:rollback(Ref).

%%--------------------------------------------------------------------------
%% Keyspace Info
%%--------------------------------------------------------------------------

-doc "Removes and returns the value for `Key`. Only available on transactional keyspaces.".
-spec take(ks(), Key :: binary()) -> result(binary()).
take({otx_ks, Ref}, Key) ->
    fjall_otx_ks:take(Ref, Key).

-doc "Returns `true` if `Key` exists in the keyspace.".
-spec contains_key(ks(), Key :: binary()) -> result(boolean()).
contains_key({ks, Ref}, Key) ->
    fjall_ks:contains_key(Ref, Key);
contains_key({otx_ks, Ref}, Key) ->
    fjall_otx_ks:contains_key(Ref, Key).

-doc "Returns the size of the value for `Key` in bytes.".
-spec size_of(ks(), Key :: binary()) -> result(non_neg_integer()).
size_of({ks, Ref}, Key) ->
    fjall_ks:size_of(Ref, Key);
size_of({otx_ks, Ref}, Key) ->
    fjall_otx_ks:size_of(Ref, Key).

-doc "Returns the approximate number of key-value pairs in the keyspace.".
-spec approximate_len(ks()) -> non_neg_integer().
approximate_len({ks, Ref}) ->
    fjall_ks:approximate_len(Ref);
approximate_len({otx_ks, Ref}) ->
    fjall_otx_ks:approximate_len(Ref).

-doc "Returns the first key-value pair in the keyspace.".
-spec first_key_value(ks()) -> result({binary(), binary()}).
first_key_value({ks, Ref}) ->
    fjall_ks:first_key_value(Ref);
first_key_value({otx_ks, Ref}) ->
    fjall_otx_ks:first_key_value(Ref).

-doc "Returns the last key-value pair in the keyspace.".
-spec last_key_value(ks()) -> result({binary(), binary()}).
last_key_value({ks, Ref}) ->
    fjall_ks:last_key_value(Ref);
last_key_value({otx_ks, Ref}) ->
    fjall_otx_ks:last_key_value(Ref).

-doc "Returns the filesystem path to the keyspace directory.".
-spec path(ks()) -> binary().
path({ks, Ref}) ->
    fjall_ks:path(Ref);
path({otx_ks, Ref}) ->
    fjall_otx_ks:path(Ref).

%%--------------------------------------------------------------------------
%% Iterators
%%--------------------------------------------------------------------------

-doc "Creates an iterator over all key-value pairs in the keyspace.".
-spec iter(ks(), direction()) -> result(iter()).
iter({ks, Ref}, Direction) ->
    fjall_ks:iter(Ref, Direction);
iter({otx_ks, Ref}, Direction) ->
    fjall_otx_ks:iter(Ref, Direction).

-doc "Creates an iterator over keys matching `Prefix`.".
-spec iter(ks(), direction(), Prefix :: binary()) -> result(iter()).
iter({ks, Ref}, Direction, Prefix) ->
    fjall_ks:iter(Ref, Direction, Prefix);
iter({otx_ks, Ref}, Direction, Prefix) ->
    fjall_otx_ks:iter(Ref, Direction, Prefix).

-doc "Creates an iterator over keys in the range `{Start, End}`.".
-spec iter(ks(), direction(), range(), {Start :: binary(), End :: binary()}) -> result(iter()).
iter({ks, Ref}, Direction, Range, {Start, End}) ->
    fjall_ks:iter(Ref, Direction, Range, {Start, End});
iter({otx_ks, Ref}, Direction, Range, {Start, End}) ->
    fjall_otx_ks:iter(Ref, Direction, Range, {Start, End}).

-doc "Returns the next key-value pair from the iterator, or `done` if exhausted.".
-spec next(iter()) -> {ok, {binary(), binary()}} | done | {error, term()}.
next(Iter) ->
    fjall_iter:next(Iter).

-doc "Returns a list of all remaining key-value pairs from the iterator.".
-spec collect(iter()) -> {ok, [{binary(), binary()}]} | {error, term()}.
collect(Iter) ->
    fjall_iter:collect(Iter).

-doc "Returns a list of up to `N` key-value pairs from the iterator.".
-spec collect(iter(), pos_integer()) -> {ok, [{binary(), binary()}]} | {error, term()}.
collect(Iter, N) ->
    fjall_iter:collect(Iter, N).

-doc "Destroys the iterator. Call this to release resources without waiting for GC.".
-spec destroy(iter()) -> ok.
destroy(Iter) ->
    fjall_iter:destroy(Iter).

%%--------------------------------------------------------------------------
%% Helper Functions (private)
%%--------------------------------------------------------------------------

-spec wrap_ks(result(fjall_ks:ks())) -> result(ks()).
wrap_ks({ok, Ref}) -> {ok, {ks, Ref}};
wrap_ks(Err) -> Err.

-spec wrap_otx_db(result(fjall_otx_db:otx_db())) -> result(db()).
wrap_otx_db({ok, Ref}) -> {ok, {otx_db, Ref}};
wrap_otx_db(Err) -> Err.

-spec wrap_otx_ks(result(fjall_otx_ks:otx_ks())) -> result(ks()).
wrap_otx_ks({ok, Ref}) -> {ok, {otx_ks, Ref}};
wrap_otx_ks(Err) -> Err.

-spec wrap_tx(result(fjall_otx_tx:write_tx())) -> result(tx()).
wrap_tx({ok, Ref}) -> {ok, {tx, Ref}};
wrap_tx(Err) -> Err.

-spec wrap_snapshot(result(fjall_snapshot:snapshot())) -> result(snapshot()).
wrap_snapshot({ok, Ref}) -> {ok, {snapshot, Ref}};
wrap_snapshot(Err) -> Err.
