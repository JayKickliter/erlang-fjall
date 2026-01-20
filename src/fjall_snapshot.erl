-module(fjall_snapshot).

-moduledoc """
Read snapshot operations for optimistic transactional databases.

This module provides operations for working with read snapshots (read
transactions) in optimistic transactional databases. Snapshots provide
snapshot isolation with repeatable read semantics.

See [Snapshot](https://docs.rs/fjall/3.0.1/fjall/struct.Snapshot.html)
in the Rust documentation.
""".

-export([get/3]).

-export_type([snapshot/0]).

-doc """
Opaque handle to a read snapshot.

Provides snapshot isolation with repeatable read semantics. Created
with `fjall_otx_db:snapshot/1`. Read-only, no explicit commit/rollback needed.
""".
-nominal snapshot() :: reference().

-doc """
Retrieves a value from a read snapshot.

Returns the value if it exists in the snapshot, `not_found` if it
doesn't, or `{error, Reason}` on other errors. All reads in the same
read snapshot see the same point-in-time view, regardless of concurrent
writes.

## Example

```erlang
{ok, Snapshot} = fjall_otx_db:snapshot(Database),
{ok, Value} = fjall_snapshot:get(Snapshot, Keyspace, <<"key">>)
```

See [Snapshot::get](https://docs.rs/fjall/3.0.1/fjall/struct.Snapshot.html#method.get)
in the Rust documentation.
""".
-spec get(
    Snapshot :: snapshot(),
    Keyspace :: fjall_otx_ks:otx_ks(),
    Key :: binary()
) -> {ok, binary()} | not_found | {error, term()}.
get(Snapshot, Keyspace, Key) ->
    case fjall_nif:snapshot_get(Snapshot, Keyspace, Key) of
        {ok, Value} -> {ok, Value};
        {error, not_found} -> not_found;
        {error, Reason} -> {error, Reason}
    end.
