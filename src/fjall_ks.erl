-module(fjall_ks).

-moduledoc """
Plain non-transactional keyspace operations.

This module provides operations for interacting with keyspaces in a plain
(non-transactional) Fjall database. Keyspaces are logical separations of
data within a database.

See [Keyspace](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html)
in the Rust documentation.
""".

-export([
    get/2,
    insert/3,
    remove/2,
    disk_space/1,
    iter/1,
    iter/2,
    range/3,
    range/4,
    prefix/2,
    prefix/3
]).

-export_type([ks/0]).

-doc """
Opaque handle to a keyspace within a plain database.

Keyspaces are logical separations of data within a database. Use
`fjall_db:keyspace/2` to open a keyspace for key-value operations.
""".
-nominal ks() :: reference().

-doc """
Retrieves the value associated with a key from the keyspace.

Returns `{ok, Value}` if the key exists, `not_found` if the key does
not exist, or `{error, Reason}` on other errors.

## Example

```erlang
case fjall_ks:get(Keyspace, <<"alice">>) of
    {ok, Email} ->
        io:format("Alice's email: ~s~n", [Email]);
    not_found ->
        io:format("Alice not found~n");
    {error, Reason} ->
        io:format("Error: ~p~n", [Reason])
end
```

See [Keyspace::get](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.get)
in the Rust documentation.
""".
-spec get(Keyspace :: ks(), Key :: binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(Keyspace, Key) ->
    case fjall:ks_get(Keyspace, Key) of
        {ok, Value} -> {ok, Value};
        {error, not_found} -> not_found;
        {error, Reason} -> {error, Reason}
    end.

-doc """
Inserts or updates a key-value pair in the keyspace.

If the key already exists, its value is overwritten. Both keys and
values must be binaries.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
ok = fjall_ks:insert(Keyspace, <<"alice">>, <<"alice@example.com">>)
```

See [Keyspace::insert](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.insert)
in the Rust documentation.
""".
-spec insert(Keyspace :: ks(), Key :: binary(), Value :: binary()) ->
    fjall:result().
insert(Keyspace, Key, Value) ->
    fjall:ks_insert(Keyspace, Key, Value).

-doc """
Removes a key-value pair from the keyspace.

If the key does not exist, this is a no-op and still returns `ok`.

Returns `ok` on success or `{error, Reason}` on failure.

## Example

```erlang
ok = fjall_ks:remove(Keyspace, <<"alice">>)
```

See [Keyspace::remove](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.remove)
in the Rust documentation.
""".
-spec remove(Keyspace :: ks(), Key :: binary()) -> fjall:result().
remove(Keyspace, Key) ->
    fjall:ks_remove(Keyspace, Key).

-doc """
Returns the approximate disk space used by the keyspace in bytes.

This is an approximation and may not reflect the exact size on disk.

## Example

```erlang
Size = fjall_ks:disk_space(Keyspace),
io:format("Keyspace size: ~p bytes~n", [Size])
```

See [Keyspace::disk_space](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.disk_space)
in the Rust documentation.
""".
-spec disk_space(Keyspace :: ks()) -> non_neg_integer().
disk_space(Keyspace) ->
    fjall:ks_disk_space(Keyspace).

-doc """
Creates an iterator over all key-value pairs in the keyspace.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace),
{ok, Items} = fjall_iter:collect(Iter)
```

See [Keyspace::iter](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.iter)
in the Rust documentation.
""".
-spec iter(ks()) -> fjall:result(fjall_iter:iter()).
iter(Ks) -> iter(Ks, []).

-doc """
Creates an iterator over all key-value pairs with options.

Options:
- `reverse` - Iterate in reverse order (from last to first)

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, [reverse]),
{ok, Items} = fjall_iter:collect(Iter)
```
""".
-spec iter(ks(), [fjall:iter_option()]) -> fjall:result(fjall_iter:iter()).
iter(Ks, Options) -> fjall:ks_iter(Ks, Options).

-doc """
Creates an iterator over a range of keys `[Start, End)`.

The range is half-open: Start is inclusive, End is exclusive.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:range(Keyspace, <<"a">>, <<"d">>),
{ok, Items} = fjall_iter:collect(Iter)
%% Returns items with keys: "a", "b", "c" (but not "d")
```

See [Keyspace::range](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.range)
in the Rust documentation.
""".
-spec range(ks(), Start :: binary(), End :: binary()) -> fjall:result(fjall_iter:iter()).
range(Ks, Start, End) -> range(Ks, Start, End, []).

-doc """
Creates an iterator over a range of keys with options.

The range is half-open: Start is inclusive, End is exclusive.

Options:
- `reverse` - Iterate in reverse order

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:range(Keyspace, <<"a">>, <<"d">>, [reverse]),
{ok, Items} = fjall_iter:collect(Iter)
%% Returns items with keys: "c", "b", "a" (but not "d")
```
""".
-spec range(ks(), Start :: binary(), End :: binary(), [fjall:iter_option()]) ->
    fjall:result(fjall_iter:iter()).
range(Ks, Start, End, Options) -> fjall:ks_range(Ks, Start, End, Options).

-doc """
Creates an iterator over keys with a given prefix.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:prefix(Keyspace, <<"user:">>),
{ok, Items} = fjall_iter:collect(Iter)
%% Returns all items with keys starting with "user:"
```

See [Keyspace::prefix](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.prefix)
in the Rust documentation.
""".
-spec prefix(ks(), binary()) -> fjall:result(fjall_iter:iter()).
prefix(Ks, Prefix) -> prefix(Ks, Prefix, []).

-doc """
Creates an iterator over keys with a given prefix with options.

Options:
- `reverse` - Iterate in reverse order

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:prefix(Keyspace, <<"user:">>, [reverse]),
{ok, Items} = fjall_iter:collect(Iter)
```
""".
-spec prefix(ks(), binary(), [fjall:iter_option()]) -> fjall:result(fjall_iter:iter()).
prefix(Ks, Prefix, Options) -> fjall:ks_prefix(Ks, Prefix, Options).
