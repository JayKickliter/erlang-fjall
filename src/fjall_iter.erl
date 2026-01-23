-module(fjall_iter).
-moduledoc "Iterator operations for keyspace scanning.".

-export([
    next/1,
    collect/1,
    collect/2,
    collect_keys/1,
    collect_keys/2,
    collect_values/1,
    collect_values/2,
    destroy/1
]).

-export_type([iter/0, kv/0]).

-doc """
Opaque handle to a keyspace iterator.

Iterators are created using `fjall_ks:iter/2`, `fjall_ks:iter/3` (prefix),
or `fjall_ks:iter/4` (range) and their equivalents in `fjall_otx_ks`.
""".
-nominal iter() :: reference().

-doc """
A key-value pair returned by iterator operations.
""".
-type kv() :: {Key :: binary(), Value :: binary()}.

-doc """
Gets the next item from the iterator.

Returns `{ok, {Key, Value}}` if there is a next item, `done` if the
iterator is exhausted, or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
case fjall_iter:next(Iter) of
    {ok, {Key, Value}} ->
        io:format("Key: ~s, Value: ~s~n", [Key, Value]);
    done ->
        io:format("No more items~n")
end
```
""".
-spec next(iter()) -> {ok, kv()} | done | {error, term()}.
next(Iter) -> fjall_nif:iter_next(Iter).

-doc """
Collects up to N items from the iterator.

Returns `{ok, Items}` where Items is a list of key-value pairs (may be
empty if iterator is exhausted), or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, Items} = fjall_iter:collect(Iter, 10),
lists:foreach(fun({K, V}) -> io:format("~s: ~s~n", [K, V]) end, Items)
```
""".
-spec collect(iter(), pos_integer()) -> {ok, [kv()]} | {error, term()}.
collect(Iter, N) -> fjall_nif:iter_collect(Iter, N).

-doc """
Collects all remaining items from the iterator.

Returns `{ok, Items}` where Items is a list of all remaining key-value
pairs, or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward, <<"user:">>),
{ok, Items} = fjall_iter:collect(Iter),
io:format("Found ~p items~n", [length(Items)])
```
""".
-spec collect(iter()) -> {ok, [kv()]} | {error, term()}.
collect(Iter) -> fjall_nif:iter_collect(Iter).

-doc """
Collects up to N keys from the iterator.

Returns `{ok, Keys}` where Keys is a list of keys (may be empty if
iterator is exhausted), or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, Keys} = fjall_iter:collect_keys(Iter, 10),
lists:foreach(fun(K) -> io:format("~s~n", [K]) end, Keys)
```
""".
-spec collect_keys(iter(), pos_integer()) -> {ok, [binary()]} | {error, term()}.
collect_keys(Iter, N) -> fjall_nif:iter_collect_keys(Iter, N).

-doc """
Collects all remaining keys from the iterator.

Returns `{ok, Keys}` where Keys is a list of all remaining keys,
or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward, <<"user:">>),
{ok, Keys} = fjall_iter:collect_keys(Iter),
io:format("Found ~p keys~n", [length(Keys)])
```
""".
-spec collect_keys(iter()) -> {ok, [binary()]} | {error, term()}.
collect_keys(Iter) -> fjall_nif:iter_collect_keys(Iter).

-doc """
Collects up to N values from the iterator.

Returns `{ok, Values}` where Values is a list of values (may be empty if
iterator is exhausted), or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, Values} = fjall_iter:collect_values(Iter, 10),
lists:foreach(fun(V) -> io:format("~s~n", [V]) end, Values)
```
""".
-spec collect_values(iter(), pos_integer()) -> {ok, [binary()]} | {error, term()}.
collect_values(Iter, N) -> fjall_nif:iter_collect_values(Iter, N).

-doc """
Collects all remaining values from the iterator.

Returns `{ok, Values}` where Values is a list of all remaining values,
or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward, <<"user:">>),
{ok, Values} = fjall_iter:collect_values(Iter),
io:format("Found ~p values~n", [length(Values)])
```
""".
-spec collect_values(iter()) -> {ok, [binary()]} | {error, term()}.
collect_values(Iter) -> fjall_nif:iter_collect_values(Iter).

-doc """
Explicitly destroys an iterator to release resources early.

Iterators are automatically cleaned up when garbage collected, but
calling destroy/1 releases the underlying resources immediately.

Returns `ok`.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, _} = fjall_iter:collect(Iter, 10),
ok = fjall_iter:destroy(Iter)
```
""".
-spec destroy(iter()) -> ok.
destroy(Iter) -> fjall_nif:iter_destroy(Iter).
