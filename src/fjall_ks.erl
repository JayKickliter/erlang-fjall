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
    contains_key/2,
    size_of/2,
    disk_space/1,
    approximate_len/1,
    first_key_value/1,
    last_key_value/1,
    path/1,
    iter/2,
    iter/3,
    iter/4
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
    case fjall_nif:ks_get(Keyspace, Key) of
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
    fjall_nif:ks_insert(Keyspace, Key, Value).

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
    fjall_nif:ks_remove(Keyspace, Key).

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
    fjall_nif:ks_disk_space(Keyspace).

-doc """
Checks if a key exists in the keyspace.

Returns `{ok, true}` if the key exists, `{ok, false}` otherwise.

## Example

```erlang
{ok, Exists} = fjall_ks:contains_key(Keyspace, <<"alice">>),
case Exists of
    true -> io:format("Alice exists~n");
    false -> io:format("Alice does not exist~n")
end
```

See [Keyspace::contains_key](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.contains_key)
in the Rust documentation.
""".
-spec contains_key(Keyspace :: ks(), Key :: binary()) ->
    fjall:result(boolean()).
contains_key(Keyspace, Key) ->
    fjall_nif:ks_contains_key(Keyspace, Key).

-doc """
Returns the size of the value for a key in bytes.

Returns `{ok, Size}` if the key exists, or `{error, not_found}` if not.

## Errors

- `{error, not_found}` - Key does not exist in the keyspace

## Example

```erlang
case fjall_ks:size_of(Keyspace, <<"alice">>) of
    {ok, Size} ->
        io:format("Alice's value is ~p bytes~n", [Size]);
    {error, not_found} ->
        io:format("Alice not found~n")
end
```

See [Keyspace::size_of](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.size_of)
in the Rust documentation.
""".
-spec size_of(Keyspace :: ks(), Key :: binary()) ->
    fjall:result(non_neg_integer()).
size_of(Keyspace, Key) ->
    fjall_nif:ks_size_of(Keyspace, Key).

-doc """
Returns the approximate number of key-value pairs in the keyspace.

This is an estimate and may not be exact due to pending compactions
and tombstones.

## Example

```erlang
Count = fjall_ks:approximate_len(Keyspace),
io:format("Keyspace has approximately ~p items~n", [Count])
```

See [Keyspace::approximate_len](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.approximate_len)
in the Rust documentation.
""".
-spec approximate_len(Keyspace :: ks()) -> non_neg_integer().
approximate_len(Keyspace) ->
    fjall_nif:ks_approximate_len(Keyspace).

-doc """
Returns the first key-value pair in the keyspace (by key order).

Returns `{ok, {Key, Value}}` if the keyspace is not empty, or
`{error, not_found}` if the keyspace is empty.

## Errors

- `{error, not_found}` - Keyspace is empty

## Example

```erlang
case fjall_ks:first_key_value(Keyspace) of
    {ok, {Key, Value}} ->
        io:format("First entry: ~s => ~s~n", [Key, Value]);
    {error, not_found} ->
        io:format("Keyspace is empty~n")
end
```

See [Keyspace::first_key_value](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.first_key_value)
in the Rust documentation.
""".
-spec first_key_value(Keyspace :: ks()) ->
    fjall:result({binary(), binary()}).
first_key_value(Keyspace) ->
    fjall_nif:ks_first_key_value(Keyspace).

-doc """
Returns the last key-value pair in the keyspace (by key order).

Returns `{ok, {Key, Value}}` if the keyspace is not empty, or
`{error, not_found}` if the keyspace is empty.

## Errors

- `{error, not_found}` - Keyspace is empty

## Example

```erlang
case fjall_ks:last_key_value(Keyspace) of
    {ok, {Key, Value}} ->
        io:format("Last entry: ~s => ~s~n", [Key, Value]);
    {error, not_found} ->
        io:format("Keyspace is empty~n")
end
```

See [Keyspace::last_key_value](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.last_key_value)
in the Rust documentation.
""".
-spec last_key_value(Keyspace :: ks()) ->
    fjall:result({binary(), binary()}).
last_key_value(Keyspace) ->
    fjall_nif:ks_last_key_value(Keyspace).

-doc """
Returns the path to the keyspace directory on disk.

## Example

```erlang
Path = fjall_ks:path(Keyspace),
io:format("Keyspace path: ~s~n", [Path])
```

See [Keyspace::path](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.path)
in the Rust documentation.
""".
-spec path(Keyspace :: ks()) -> binary().
path(Keyspace) ->
    fjall_nif:ks_path(Keyspace).

-doc """
Creates an iterator over all key-value pairs in the keyspace.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, Items} = fjall_iter:collect(Iter)
```

See [Keyspace::iter](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.iter)
in the Rust documentation.
""".
-spec iter(ks(), fjall:direction()) -> fjall:result(fjall_iter:iter()).
iter(Ks, Direction) -> fjall_nif:ks_iter(Ks, Direction).

-doc """
Creates an iterator over a range of keys.

The range boundary type is controlled by the `Range` parameter:
- `inclusive` - Range is `[Start, End]` (end is included)
- `exclusive` - Range is `[Start, End)` (end is excluded)

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
%% Exclusive range [a, d) - returns "a", "b", "c" but not "d"
{ok, Iter1} = fjall_ks:iter(Keyspace, forward, exclusive, {<<"a">>, <<"d">>}),

%% Inclusive range [a, d] - returns "a", "b", "c", "d"
{ok, Iter2} = fjall_ks:iter(Keyspace, forward, inclusive, {<<"a">>, <<"d">>}),

%% Reverse order
{ok, Iter3} = fjall_ks:iter(Keyspace, reverse, exclusive, {<<"a">>, <<"d">>})
```

See [Keyspace::range](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.range)
in the Rust documentation.
""".
-spec iter(ks(), fjall:direction(), fjall:range(), {Start :: binary(), End :: binary()}) ->
    fjall:result(fjall_iter:iter()).
iter(Ks, Direction, Range, {Start, End}) -> fjall_nif:ks_range(Ks, Direction, Range, Start, End).

-doc """
Creates an iterator over keys with a given prefix.

Returns `{ok, Iterator}` on success or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward, <<"user:">>),
{ok, Items} = fjall_iter:collect(Iter)
%% Returns all items with keys starting with "user:"
```

See [Keyspace::prefix](https://docs.rs/fjall/3.0.1/fjall/struct.Keyspace.html#method.prefix)
in the Rust documentation.
""".
-spec iter(ks(), fjall:direction(), Prefix :: binary()) -> fjall:result(fjall_iter:iter()).
iter(Ks, Direction, Prefix) -> fjall_nif:ks_prefix(Ks, Direction, Prefix).
