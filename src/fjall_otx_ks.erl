-module(fjall_otx_ks).

-moduledoc """
Optimistic transactional keyspace (type-only module).

This module defines the `otx_ks()` type for keyspaces in optimistic
transactional databases. Operations on transactional keyspaces are performed
via transactions (see `fjall_otx_tx`) or snapshots (see `fjall_snapshot`).

See [OptimisticTxKeyspace](https://docs.rs/fjall/3.0.1/fjall/struct.OptimisticTxKeyspace.html)
in the Rust documentation.
""".

-export_type([otx_ks/0]).

-doc """
Opaque handle to a keyspace in an optimistic transactional database.

Keyspaces in transactional databases can only be accessed via transactions
or snapshots. Use `fjall_otx_db:keyspace/2` to open a transactional keyspace,
then use `fjall_otx_tx` or `fjall_snapshot` modules for operations.
""".
-nominal otx_ks() :: reference().
