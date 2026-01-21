-module(fjall_otx_ks).

-moduledoc """
Optimistic transactional keyspace operations.

This module provides convenience methods for optimistic transactional keyspaces
that auto-wrap single operations in transactions. For multi-operation transactions,
use `fjall_otx_tx` instead.

See [OptimisticTxKeyspace](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html)
in the Rust documentation.
""".

-export([
    insert/3,
    get/2,
    remove/2,
    take/2,
    contains_key/2,
    size_of/2,
    approximate_len/1,
    first_key_value/1,
    last_key_value/1,
    path/1,
    iter/2,
    range/5,
    prefix/3
]).

-export_type([otx_ks/0]).

-doc """
Opaque handle to a keyspace in an optimistic transactional database.

Use `fjall_otx_db:keyspace/2` to open a transactional keyspace. Operations
on this keyspace can be performed directly using the functions in this module
(which auto-wrap in transactions), or via explicit transactions using
`fjall_otx_tx`, or via snapshots using `fjall_snapshot`.
""".
-nominal otx_ks() :: reference().

-doc """
Inserts or updates a key-value pair in the keyspace.

This is a convenience method that auto-wraps the operation in a transaction.
If the key already exists, its value is overwritten.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
ok = fjall_otx_ks:insert(Keyspace, <<"alice">>, <<"alice@example.com">>)
```

See [OptimisticTxKeyspace::insert](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.insert)
in the Rust documentation.
""".
-spec insert(Keyspace :: otx_ks(), Key :: binary(), Value :: binary()) ->
    fjall:result().
insert(Keyspace, Key, Value) ->
    fjall_nif:otx_ks_insert(Keyspace, Key, Value).

-doc """
Retrieves the value associated with a key from the keyspace.

This is a convenience method that auto-wraps the operation in a transaction.
Returns `{ok, Value}` if the key exists, `not_found` if the key does not
exist, or `{error, Reason}` on other errors.

## Example

```erlang
case fjall_otx_ks:get(Keyspace, <<"alice">>) of
    {ok, Email} ->
        io:format("Alice's email: ~s~n", [Email]);
    not_found ->
        io:format("Alice not found~n")
end
```

See [OptimisticTxKeyspace::get](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.get)
in the Rust documentation.
""".
-spec get(Keyspace :: otx_ks(), Key :: binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(Keyspace, Key) ->
    case fjall_nif:otx_ks_get(Keyspace, Key) of
        {ok, Value} -> {ok, Value};
        {error, not_found} -> not_found;
        {error, Reason} -> {error, Reason}
    end.

-doc """
Removes a key-value pair from the keyspace.

This is a convenience method that auto-wraps the operation in a transaction.
If the key does not exist, this is a no-op and still returns `ok`.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
ok = fjall_otx_ks:remove(Keyspace, <<"alice">>)
```

See [OptimisticTxKeyspace::remove](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.remove)
in the Rust documentation.
""".
-spec remove(Keyspace :: otx_ks(), Key :: binary()) -> fjall:result().
remove(Keyspace, Key) ->
    fjall_nif:otx_ks_remove(Keyspace, Key).

-doc """
Removes and returns the value associated with a key.

This is a convenience method that auto-wraps the operation in a transaction.
Returns `{ok, Value}` if the key existed and was removed, or `{error, not_found}`
if the key did not exist.

## Errors

- `{error, not_found}` - Key does not exist in the keyspace

## Example

```erlang
case fjall_otx_ks:take(Keyspace, <<"alice">>) of
    {ok, Email} ->
        io:format("Removed Alice with email: ~s~n", [Email]);
    {error, not_found} ->
        io:format("Alice not found~n")
end
```

See [OptimisticTxKeyspace::take](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.take)
in the Rust documentation.
""".
-spec take(Keyspace :: otx_ks(), Key :: binary()) -> fjall:result(binary()).
take(Keyspace, Key) ->
    fjall_nif:otx_ks_take(Keyspace, Key).

-doc """
Checks if a key exists in the keyspace.

This is a convenience method that auto-wraps the operation in a transaction.
Returns `{ok, true}` if the key exists, `{ok, false}` otherwise.

## Example

```erlang
{ok, Exists} = fjall_otx_ks:contains_key(Keyspace, <<"alice">>),
case Exists of
    true -> io:format("Alice exists~n");
    false -> io:format("Alice does not exist~n")
end
```

See [OptimisticTxKeyspace::contains_key](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.contains_key)
in the Rust documentation.
""".
-spec contains_key(Keyspace :: otx_ks(), Key :: binary()) ->
    fjall:result(boolean()).
contains_key(Keyspace, Key) ->
    fjall_nif:otx_ks_contains_key(Keyspace, Key).

-doc """
Returns the size of the value for a key in bytes.

This is a convenience method that auto-wraps the operation in a transaction.
Returns `{ok, Size}` if the key exists, or `{error, not_found}` if not.

## Errors

- `{error, not_found}` - Key does not exist in the keyspace

## Example

```erlang
case fjall_otx_ks:size_of(Keyspace, <<"alice">>) of
    {ok, Size} ->
        io:format("Alice's value is ~p bytes~n", [Size]);
    {error, not_found} ->
        io:format("Alice not found~n")
end
```

See [OptimisticTxKeyspace::size_of](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.size_of)
in the Rust documentation.
""".
-spec size_of(Keyspace :: otx_ks(), Key :: binary()) ->
    fjall:result(non_neg_integer()).
size_of(Keyspace, Key) ->
    fjall_nif:otx_ks_size_of(Keyspace, Key).

-doc """
Returns the approximate number of key-value pairs in the keyspace.

This is an estimate and may not be exact due to pending compactions
and tombstones.

## Example

```erlang
Count = fjall_otx_ks:approximate_len(Keyspace),
io:format("Keyspace has approximately ~p items~n", [Count])
```

See [OptimisticTxKeyspace::approximate_len](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.approximate_len)
in the Rust documentation.
""".
-spec approximate_len(Keyspace :: otx_ks()) -> non_neg_integer().
approximate_len(Keyspace) ->
    fjall_nif:otx_ks_approximate_len(Keyspace).

-doc """
Returns the first key-value pair in the keyspace (by key order).

Returns `{ok, {Key, Value}}` if the keyspace is not empty, or
`{error, not_found}` if the keyspace is empty.

## Errors

- `{error, not_found}` - Keyspace is empty

## Example

```erlang
case fjall_otx_ks:first_key_value(Keyspace) of
    {ok, {Key, Value}} ->
        io:format("First entry: ~s => ~s~n", [Key, Value]);
    {error, not_found} ->
        io:format("Keyspace is empty~n")
end
```

See [OptimisticTxKeyspace::first_key_value](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.first_key_value)
in the Rust documentation.
""".
-spec first_key_value(Keyspace :: otx_ks()) ->
    fjall:result({binary(), binary()}).
first_key_value(Keyspace) ->
    fjall_nif:otx_ks_first_key_value(Keyspace).

-doc """
Returns the last key-value pair in the keyspace (by key order).

Returns `{ok, {Key, Value}}` if the keyspace is not empty, or
`{error, not_found}` if the keyspace is empty.

## Errors

- `{error, not_found}` - Keyspace is empty

## Example

```erlang
case fjall_otx_ks:last_key_value(Keyspace) of
    {ok, {Key, Value}} ->
        io:format("Last entry: ~s => ~s~n", [Key, Value]);
    {error, not_found} ->
        io:format("Keyspace is empty~n")
end
```

See [OptimisticTxKeyspace::last_key_value](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.last_key_value)
in the Rust documentation.
""".
-spec last_key_value(Keyspace :: otx_ks()) ->
    fjall:result({binary(), binary()}).
last_key_value(Keyspace) ->
    fjall_nif:otx_ks_last_key_value(Keyspace).

-doc """
Returns the path to the keyspace directory on disk.

## Example

```erlang
Path = fjall_otx_ks:path(Keyspace),
io:format("Keyspace path: ~s~n", [Path])
```

See [OptimisticTxKeyspace::path](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.path)
in the Rust documentation.
""".
-spec path(Keyspace :: otx_ks()) -> binary().
path(Keyspace) ->
    fjall_nif:otx_ks_path(Keyspace).

-doc """
Creates an iterator over all key-value pairs in the keyspace.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_otx_ks:iter(Keyspace, forward),
{ok, Items} = fjall_iter:collect(Iter)
```

See [OptimisticTxKeyspace::iter](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.iter)
in the Rust documentation.
""".
-spec iter(otx_ks(), fjall:direction()) -> fjall:result(fjall_iter:iter()).
iter(Ks, Direction) -> fjall_nif:otx_ks_iter(Ks, Direction).

-doc """
Creates an iterator over a range of keys.

The range boundary type is controlled by the `Range` parameter:
- `inclusive` - Range is `[Start, End]` (end is included)
- `exclusive` - Range is `[Start, End)` (end is excluded)

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
%% Exclusive range [a, d) - returns "a", "b", "c" but not "d"
{ok, Iter1} = fjall_otx_ks:range(Keyspace, forward, exclusive, <<"a">>, <<"d">>),

%% Inclusive range [a, d] - returns "a", "b", "c", "d"
{ok, Iter2} = fjall_otx_ks:range(Keyspace, forward, inclusive, <<"a">>, <<"d">>),

%% Reverse order
{ok, Iter3} = fjall_otx_ks:range(Keyspace, reverse, exclusive, <<"a">>, <<"d">>)
```

See [OptimisticTxKeyspace::range](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.range)
in the Rust documentation.
""".
-spec range(otx_ks(), fjall:direction(), fjall:range(), Start :: binary(), End :: binary()) ->
    fjall:result(fjall_iter:iter()).
range(Ks, Direction, Range, Start, End) -> fjall_nif:otx_ks_range(Ks, Direction, Range, Start, End).

-doc """
Creates an iterator over keys with a given prefix.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_otx_ks:prefix(Keyspace, forward, <<"user:">>),
{ok, Items} = fjall_iter:collect(Iter)
%% Returns all items with keys starting with "user:"
```

See [OptimisticTxKeyspace::prefix](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html#method.prefix)
in the Rust documentation.
""".
-spec prefix(otx_ks(), fjall:direction(), binary()) -> fjall:result(fjall_iter:iter()).
prefix(Ks, Direction, Prefix) -> fjall_nif:otx_ks_prefix(Ks, Direction, Prefix).
