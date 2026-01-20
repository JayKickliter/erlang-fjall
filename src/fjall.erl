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
    compression/0,
    config_option/0,
    iter_option/0,
    ks_option/0,
    persist_mode/0,
    result/0,
    result/1
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
    commit/1, commit/2,
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
    iter/1, iter/2,
    range/3, range/4,
    prefix/2, prefix/3,
    next/1,
    collect/1,
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
Iterator option.

Supported options:

- `reverse` - Iterate in reverse order (from last to first)
""".
-type iter_option() :: reverse.

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

%%--------------------------------------------------------------------------
%% Database
%%--------------------------------------------------------------------------

-spec open(Path :: file:name_all()) -> result({db, reference()} | {otx_db, reference()}).
open(Path) ->
    open(Path, []).

-spec open(Path :: file:name_all(), Options :: [config_option() | {optimistic, boolean()}]) ->
    result({db, reference()} | {otx_db, reference()}).
open(Path, Options) ->
    case proplists:get_value(optimistic, Options, false) of
        true ->
            OptsWithoutTx = proplists:delete(optimistic, Options),
            wrap_otx_db(fjall_otx_db:open(Path, OptsWithoutTx));
        false ->
            case fjall_db:open(Path, Options) of
                {ok, Ref} -> {ok, {db, Ref}};
                Err -> Err
            end
    end.

-spec batch({db, reference()}) -> result({batch, reference()}).
batch({db, Ref}) ->
    case fjall_db:batch(Ref) of
        {ok, BatchRef} -> {ok, {batch, BatchRef}};
        Err -> Err
    end.

-spec persist({db, reference()}, Mode :: persist_mode()) -> result().
persist({db, Ref}, Mode) ->
    fjall_db:persist(Ref, Mode);
persist({otx_db, Ref}, Mode) ->
    fjall_otx_db:persist(Ref, Mode).

-spec keyspace({db, reference()} | {otx_db, reference()}, Name :: binary()) ->
    result({ks, reference()} | {otx_ks, reference()}).
keyspace({db, Ref}, Name) ->
    wrap_ks(fjall_db:keyspace(Ref, Name));
keyspace({otx_db, Ref}, Name) ->
    wrap_otx_ks(fjall_otx_db:keyspace(Ref, Name)).

-spec keyspace({db, reference()} | {otx_db, reference()}, Name :: binary(), Opts :: [ks_option()]) ->
    result({ks, reference()} | {otx_ks, reference()}).
keyspace({db, Ref}, Name, Opts) ->
    wrap_ks(fjall_db:keyspace(Ref, Name, Opts));
keyspace({otx_db, Ref}, Name, Opts) ->
    wrap_otx_ks(fjall_otx_db:keyspace(Ref, Name, Opts)).

-spec write_tx({otx_db, reference()}) -> result({tx, reference()}).
write_tx({otx_db, Ref}) ->
    wrap_tx(fjall_otx_db:write_tx(Ref)).

-spec snapshot({otx_db, reference()}) -> result({snapshot, reference()}).
snapshot({otx_db, Ref}) ->
    wrap_snapshot(fjall_otx_db:snapshot(Ref)).

%%--------------------------------------------------------------------------
%% Key-Value Operations
%%--------------------------------------------------------------------------

-spec get({ks, reference()} | {otx_ks, reference()}, Key :: binary()) -> result(binary()).
get({ks, Ref}, Key) ->
    fjall_ks:get(Ref, Key);
get({otx_ks, Ref}, Key) ->
    fjall_otx_ks:get(Ref, Key).

-spec get
    ({tx, reference()}, {otx_ks, reference()}, Key :: binary()) -> result(binary());
    ({snapshot, reference()}, {otx_ks, reference()}, Key :: binary()) -> result(binary()).
get({tx, TxRef}, {otx_ks, KsRef}, Key) ->
    fjall_otx_tx:get(TxRef, KsRef, Key);
get({snapshot, SnapRef}, {otx_ks, KsRef}, Key) ->
    fjall_snapshot:get(SnapRef, KsRef, Key).

-spec insert({ks, reference()} | {otx_ks, reference()}, Key :: binary(), Value :: binary()) ->
    result().
insert({ks, Ref}, Key, Value) ->
    fjall_ks:insert(Ref, Key, Value);
insert({otx_ks, Ref}, Key, Value) ->
    fjall_otx_ks:insert(Ref, Key, Value).

-spec insert
    ({batch, reference()}, {ks, reference()}, Key :: binary(), Value :: binary()) -> result();
    ({tx, reference()}, {otx_ks, reference()}, Key :: binary(), Value :: binary()) -> result().
insert({batch, BRef}, {ks, KsRef}, Key, Value) ->
    fjall_wb:insert(BRef, KsRef, Key, Value);
insert({tx, TxRef}, {otx_ks, KsRef}, Key, Value) ->
    fjall_otx_tx:insert(TxRef, KsRef, Key, Value).

-spec remove({ks, reference()} | {otx_ks, reference()}, Key :: binary()) -> result().
remove({ks, Ref}, Key) ->
    fjall_ks:remove(Ref, Key);
remove({otx_ks, Ref}, Key) ->
    fjall_otx_ks:remove(Ref, Key).

-spec remove
    ({batch, reference()}, {ks, reference()}, Key :: binary()) -> result();
    ({tx, reference()}, {otx_ks, reference()}, Key :: binary()) -> result().
remove({batch, BRef}, {ks, KsRef}, Key) ->
    fjall_wb:remove(BRef, KsRef, Key);
remove({tx, TxRef}, {otx_ks, KsRef}, Key) ->
    fjall_otx_tx:remove(TxRef, KsRef, Key).

-spec disk_space({ks, reference()}) -> non_neg_integer().
disk_space({ks, Ref}) ->
    fjall_ks:disk_space(Ref).

-spec iter({ks, reference()} | {otx_ks, reference()}) -> result({iter, reference()}).
iter({ks, Ref}) ->
    wrap_iter(fjall_ks:iter(Ref));
iter({otx_ks, Ref}) ->
    wrap_iter(fjall_otx_ks:iter(Ref)).

-spec iter({ks, reference()} | {otx_ks, reference()}, Opts :: [iter_option()]) ->
    result({iter, reference()}).
iter({ks, Ref}, Opts) ->
    wrap_iter(fjall_ks:iter(Ref, Opts));
iter({otx_ks, Ref}, Opts) ->
    wrap_iter(fjall_otx_ks:iter(Ref, Opts)).

-spec range({ks, reference()} | {otx_ks, reference()}, Start :: binary(), End :: binary()) ->
    result({iter, reference()}).
range({ks, Ref}, Start, End) ->
    wrap_iter(fjall_ks:range(Ref, Start, End));
range({otx_ks, Ref}, Start, End) ->
    wrap_iter(fjall_otx_ks:range(Ref, Start, End)).

-spec range(
    {ks, reference()} | {otx_ks, reference()},
    Start :: binary(),
    End :: binary(),
    Opts :: [iter_option()]
) -> result({iter, reference()}).
range({ks, Ref}, Start, End, Opts) ->
    wrap_iter(fjall_ks:range(Ref, Start, End, Opts));
range({otx_ks, Ref}, Start, End, Opts) ->
    wrap_iter(fjall_otx_ks:range(Ref, Start, End, Opts)).

-spec prefix({ks, reference()} | {otx_ks, reference()}, Prefix :: binary()) ->
    result({iter, reference()}).
prefix({ks, Ref}, Prefix) ->
    wrap_iter(fjall_ks:prefix(Ref, Prefix));
prefix({otx_ks, Ref}, Prefix) ->
    wrap_iter(fjall_otx_ks:prefix(Ref, Prefix)).

-spec prefix(
    {ks, reference()} | {otx_ks, reference()}, Prefix :: binary(), Opts :: [iter_option()]
) ->
    result({iter, reference()}).
prefix({ks, Ref}, Prefix, Opts) ->
    wrap_iter(fjall_ks:prefix(Ref, Prefix, Opts));
prefix({otx_ks, Ref}, Prefix, Opts) ->
    wrap_iter(fjall_otx_ks:prefix(Ref, Prefix, Opts)).

%%--------------------------------------------------------------------------
%% Batch/Transaction
%%--------------------------------------------------------------------------

-spec commit({batch, reference()} | {tx, reference()}) -> result().
commit({batch, Ref}) ->
    fjall_wb:commit(Ref);
commit({tx, Ref}) ->
    fjall_otx_tx:commit(Ref).

-spec commit({batch, reference()}, Mode :: persist_mode()) -> result().
commit({batch, Ref}, Mode) ->
    fjall_wb:commit(Ref, Mode).

-spec len({batch, reference()}) -> non_neg_integer().
len({batch, Ref}) ->
    fjall_wb:len(Ref).

-spec is_empty({batch, reference()}) -> boolean().
is_empty({batch, Ref}) ->
    fjall_wb:is_empty(Ref).

-spec rollback({tx, reference()}) -> result().
rollback({tx, Ref}) ->
    fjall_otx_tx:rollback(Ref).

%%--------------------------------------------------------------------------
%% Keyspace Info
%%--------------------------------------------------------------------------

-spec take
    ({otx_ks, reference()}, Key :: binary()) -> result(binary());
    ({iter, reference()}, N :: pos_integer()) -> {ok, [{binary(), binary()}]} | {error, term()}.
take({otx_ks, Ref}, Key) ->
    fjall_otx_ks:take(Ref, Key);
take({iter, Ref}, N) ->
    fjall_iter:take(Ref, N).

-spec contains_key({otx_ks, reference()}, Key :: binary()) -> result(boolean()).
contains_key({otx_ks, Ref}, Key) ->
    fjall_otx_ks:contains_key(Ref, Key).

-spec size_of({otx_ks, reference()}, Key :: binary()) -> result(non_neg_integer()).
size_of({otx_ks, Ref}, Key) ->
    fjall_otx_ks:size_of(Ref, Key).

-spec approximate_len({otx_ks, reference()}) -> non_neg_integer().
approximate_len({otx_ks, Ref}) ->
    fjall_otx_ks:approximate_len(Ref).

-spec first_key_value({otx_ks, reference()}) -> result({binary(), binary()}).
first_key_value({otx_ks, Ref}) ->
    fjall_otx_ks:first_key_value(Ref).

-spec last_key_value({otx_ks, reference()}) -> result({binary(), binary()}).
last_key_value({otx_ks, Ref}) ->
    fjall_otx_ks:last_key_value(Ref).

-spec path({otx_ks, reference()}) -> binary().
path({otx_ks, Ref}) ->
    fjall_otx_ks:path(Ref).

%%--------------------------------------------------------------------------
%% Iterators
%%--------------------------------------------------------------------------

-spec next({iter, reference()}) -> {ok, {binary(), binary()}} | done | {error, term()}.
next({iter, Ref}) ->
    fjall_iter:next(Ref).

-spec collect({iter, reference()}) -> {ok, [{binary(), binary()}]} | {error, term()}.
collect({iter, Ref}) ->
    fjall_iter:collect(Ref).

-spec destroy({iter, reference()}) -> ok.
destroy({iter, Ref}) ->
    fjall_iter:destroy(Ref).

%%--------------------------------------------------------------------------
%% Helper Functions (private)
%%--------------------------------------------------------------------------

-spec wrap_ks(result(reference())) -> result({ks, reference()}).
wrap_ks({ok, Ref}) -> {ok, {ks, Ref}};
wrap_ks(Err) -> Err.

-spec wrap_iter(result(reference())) -> result({iter, reference()}).
wrap_iter({ok, Ref}) -> {ok, {iter, Ref}};
wrap_iter(Err) -> Err.

-spec wrap_otx_db(result(reference())) -> result({otx_db, reference()}).
wrap_otx_db({ok, Ref}) -> {ok, {otx_db, Ref}};
wrap_otx_db(Err) -> Err.

-spec wrap_otx_ks(result(reference())) -> result({otx_ks, reference()}).
wrap_otx_ks({ok, Ref}) -> {ok, {otx_ks, Ref}};
wrap_otx_ks(Err) -> Err.

-spec wrap_tx(result(reference())) -> result({tx, reference()}).
wrap_tx({ok, Ref}) -> {ok, {tx, Ref}};
wrap_tx(Err) -> Err.

-spec wrap_snapshot(result(reference())) -> result({snapshot, reference()}).
wrap_snapshot({ok, Ref}) -> {ok, {snapshot, Ref}};
wrap_snapshot(Err) -> Err.
