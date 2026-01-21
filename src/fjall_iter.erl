-module(fjall_iter).
-moduledoc "Iterator operations for keyspace scanning.".

-export([
    next/1,
    take/2,
    collect/1,
    destroy/1
]).

-export_type([iter/0, kv/0]).

-doc """
Opaque handle to a keyspace iterator.

Iterators are created using `fjall_ks:iter/2`, `fjall_ks:range/5`,
`fjall_ks:prefix/3` or their equivalents in `fjall_otx_ks`.
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
Takes up to N items from the iterator.

Returns `{ok, Items}` where Items is a list of key-value pairs (may be
empty if iterator is exhausted), or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, Items} = fjall_iter:take(Iter, 10),
lists:foreach(fun({K, V}) -> io:format("~s: ~s~n", [K, V]) end, Items)
```
""".
-spec take(iter(), pos_integer()) -> {ok, [kv()]} | {error, term()}.
take(Iter, N) -> fjall_nif:iter_collect(Iter, N).

-doc """
Collects all remaining items from the iterator.

Returns `{ok, Items}` where Items is a list of all remaining key-value
pairs, or `{error, Reason}` on failure.

## Example

```erlang
{ok, Iter} = fjall_ks:prefix(Keyspace, <<"user:">>, forward),
{ok, Items} = fjall_iter:collect(Iter),
io:format("Found ~p items~n", [length(Items)])
```
""".
-spec collect(iter()) -> {ok, [kv()]} | {error, term()}.
collect(Iter) -> fjall_nif:iter_collect(Iter).

-doc """
Explicitly destroys an iterator to release resources early.

Iterators are automatically cleaned up when garbage collected, but
calling destroy/1 releases the underlying resources immediately.

Returns `ok`.

## Example

```erlang
{ok, Iter} = fjall_ks:iter(Keyspace, forward),
{ok, _} = fjall_iter:take(Iter, 10),
ok = fjall_iter:destroy(Iter)
```
""".
-spec destroy(iter()) -> ok.
destroy(Iter) -> fjall_nif:iter_destroy(Iter).
