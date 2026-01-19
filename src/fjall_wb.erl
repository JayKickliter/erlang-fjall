-module(fjall_wb).

-moduledoc """
Write batch operations for atomic multi-keyspace writes.

Write batches allow grouping multiple write operations across different
keyspaces into a single atomic operation. This is useful when you need
to ensure that multiple writes either all succeed or all fail together.

See [OwnedWriteBatch](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html)
in the Rust documentation.
""".

-export([
    insert/4,
    remove/3,
    commit/1,
    commit/2,
    len/1,
    is_empty/1
]).

-export_type([wb/0]).

-doc """
Opaque handle to a write batch.

Write batches are created via `fjall_db:batch/1` and can be used to
group multiple write operations atomically.
""".
-nominal wb() :: reference().

-doc """
Inserts a key-value pair into the batch for a specific keyspace.

The write is buffered in the batch and will only be applied when
`commit/1` or `commit/2` is called.

Returns `ok` on success.

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:insert(Batch, Keyspace1, <<"key1">>, <<"value1">>),
ok = fjall_wb:insert(Batch, Keyspace2, <<"key2">>, <<"value2">>),
ok = fjall_wb:commit(Batch)
```

See [OwnedWriteBatch::insert](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html#method.insert)
in the Rust documentation.
""".
-spec insert(
    Batch :: wb(),
    Keyspace :: fjall_ks:ks(),
    Key :: binary(),
    Value :: binary()
) -> ok.
insert(Batch, Keyspace, Key, Value) ->
    fjall:wb_insert(Batch, Keyspace, Key, Value).

-doc """
Removes a key from the batch for a specific keyspace.

The removal is buffered in the batch and will only be applied when
`commit/1` or `commit/2` is called.

Returns `ok` on success.

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:remove(Batch, Keyspace, <<"key">>),
ok = fjall_wb:commit(Batch)
```

See [OwnedWriteBatch::remove](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html#method.remove)
in the Rust documentation.
""".
-spec remove(Batch :: wb(), Keyspace :: fjall_ks:ks(), Key :: binary()) -> ok.
remove(Batch, Keyspace, Key) ->
    fjall:wb_remove(Batch, Keyspace, Key).

-doc """
Commits the batch with default persistence mode.

Atomically applies all buffered writes to the database. After commit,
the batch is consumed and cannot be reused.

Returns `ok` on success or `{error, Reason}` on failure.

## Errors

- `{error, batch_already_committed}` - The batch was already committed

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:insert(Batch, Keyspace, <<"key">>, <<"value">>),
ok = fjall_wb:commit(Batch)
```

See [OwnedWriteBatch::commit](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html#method.commit)
in the Rust documentation.
""".
-spec commit(Batch :: wb()) -> fjall:result().
commit(Batch) ->
    fjall:wb_commit(Batch).

-doc """
Commits the batch with a specified persistence mode.

Like `commit/1`, but allows specifying the durability guarantee.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:insert(Batch, Keyspace, <<"key">>, <<"value">>),
ok = fjall_wb:commit(Batch, sync_all)
```

See `fjall:persist_mode/0` for available persist modes.
""".
-spec commit(Batch :: wb(), Mode :: fjall:persist_mode()) -> fjall:result().
commit(Batch, Mode) ->
    fjall:wb_commit_with_mode(Batch, Mode).

-doc """
Returns the number of operations in the batch.

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
ok = fjall_wb:insert(Batch, Keyspace, <<"key1">>, <<"value1">>),
ok = fjall_wb:insert(Batch, Keyspace, <<"key2">>, <<"value2">>),
2 = fjall_wb:len(Batch)
```

See [OwnedWriteBatch::len](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html#method.len)
in the Rust documentation.
""".
-spec len(Batch :: wb()) -> non_neg_integer().
len(Batch) ->
    fjall:wb_len(Batch).

-doc """
Returns `true` if the batch contains no operations.

## Example

```erlang
{ok, Batch} = fjall_db:batch(Database),
true = fjall_wb:is_empty(Batch),
ok = fjall_wb:insert(Batch, Keyspace, <<"key">>, <<"value">>),
false = fjall_wb:is_empty(Batch)
```

See [OwnedWriteBatch::is_empty](https://docs.rs/fjall/3.0.1/fjall/struct.OwnedWriteBatch.html#method.is_empty)
in the Rust documentation.
""".
-spec is_empty(Batch :: wb()) -> boolean().
is_empty(Batch) ->
    fjall:wb_is_empty(Batch).
