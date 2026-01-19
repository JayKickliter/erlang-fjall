-module(fjall_otx_tx).

-moduledoc """
Write transaction operations for optimistic transactional databases.

This module provides operations for working with write transactions in
optimistic transactional databases. Transactions provide ACID guarantees
with optimistic concurrency control.

See [OptimisticWriteTx](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html)
in the Rust documentation.
""".

-export([
    insert/4,
    get/3,
    remove/3,
    commit/1,
    rollback/1
]).

-export_type([write_tx/0]).

-doc """
Opaque handle to a write transaction.

Provides single-writer serialized transactions with read-your-own-writes
semantics. Created with `fjall_otx_db:write_tx/1`, must be committed with
`commit/1` or rolled back with `rollback/1`.
""".
-nominal write_tx() :: reference().

-doc """
Inserts or updates a key-value pair within a write transaction.

If the key already exists, its value is updated. The write is visible
to subsequent reads within the same transaction (read-your-own-writes).

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall_otx_tx:get(Txn, Keyspace, <<"key">>),
ok = fjall_otx_tx:commit(Txn)
```

See [OptimisticWriteTx::insert](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html#method.insert)
in the Rust documentation.
""".
-spec insert(
    Txn :: write_tx(),
    Keyspace :: fjall_otx_ks:otx_ks(),
    Key :: binary(),
    Value :: binary()
) -> fjall:result().
insert(Txn, Keyspace, Key, Value) ->
    fjall:otx_tx_insert(Txn, Keyspace, Key, Value).

-doc """
Retrieves a value from a write transaction.

Returns the value if it exists (including writes made earlier in the
same transaction). Returns `{error, not_found}` if the key doesn't exist.

## Read-Your-Own-Writes

If the key was inserted or updated earlier in the same transaction,
this returns that value. Otherwise, it returns the value from the
database at transaction start time.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace, <<"key">>, <<"value">>),
{ok, <<"value">>} = fjall_otx_tx:get(Txn, Keyspace, <<"key">>)
```

See [OptimisticWriteTx::get](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html#method.get)
in the Rust documentation.
""".
-spec get(Txn :: write_tx(), Keyspace :: fjall_otx_ks:otx_ks(), Key :: binary()) ->
    fjall:result(binary()).
get(Txn, Keyspace, Key) ->
    fjall:otx_tx_get(Txn, Keyspace, Key).

-doc """
Removes a key from a write transaction.

If the key doesn't exist, this is a no-op and returns `ok`. The
removal is visible to subsequent reads in the same transaction.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:remove(Txn, Keyspace, <<"key">>),
ok = fjall_otx_tx:commit(Txn)
```

See [OptimisticWriteTx::remove](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html#method.remove)
in the Rust documentation.
""".
-spec remove(Txn :: write_tx(), Keyspace :: fjall_otx_ks:otx_ks(), Key :: binary()) ->
    fjall:result().
remove(Txn, Keyspace, Key) ->
    fjall:otx_tx_remove(Txn, Keyspace, Key).

-doc """
Commits a write transaction, making all changes durable.

Atomically applies all changes made in the transaction. On success,
returns `ok`. On failure, returns an error and the transaction is
rolled back.

After commit, the transaction handle is invalid and cannot be used
for further operations.

## Errors

- `{error, transaction_conflict}` - A concurrent transaction modified
  a key that was read by this transaction. The transaction is rolled
  back; retry the operation if appropriate.
- `{error, transaction_already_finalized}` - The transaction was
  already committed or rolled back.
- `{error, Reason}` - I/O error when writing to disk.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace, <<"key">>, <<"value">>),
ok = fjall_otx_tx:commit(Txn)
```

See [OptimisticWriteTx::commit](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html#method.commit)
in the Rust documentation.
""".
-spec commit(Txn :: write_tx()) -> fjall:result().
commit(Txn) ->
    fjall:otx_tx_commit(Txn).

-doc """
Rolls back a write transaction, discarding all changes.

After rollback, the transaction handle is invalid and cannot be used
for further operations.

Returns `ok` on success.

## Example

```erlang
{ok, Txn} = fjall_otx_db:write_tx(Database),
ok = fjall_otx_tx:insert(Txn, Keyspace, <<"key">>, <<"value">>),
ok = fjall_otx_tx:rollback(Txn)
```

See [OptimisticWriteTx::rollback](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticWriteTx.html#method.rollback)
in the Rust documentation.
""".
-spec rollback(Txn :: write_tx()) -> fjall:result().
rollback(Txn) ->
    fjall:otx_tx_rollback(Txn).
