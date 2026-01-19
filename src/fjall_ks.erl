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
    disk_space/1
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

Returns `{ok, Value}` if the key exists, or `{error, not_found}` if
the key does not exist. Returns `{error, Reason}` on other errors.

## Errors

- `{error, not_found}` - Key does not exist in the keyspace

## Example

```erlang
case fjall_ks:get(Keyspace, <<"alice">>) of
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
-spec get(Keyspace :: ks(), Key :: binary()) -> fjall:result(binary()).
get(Keyspace, Key) ->
    fjall:ks_get(Keyspace, Key).

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
