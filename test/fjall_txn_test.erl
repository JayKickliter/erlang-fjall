-module(fjall_txn_test).
-include_lib("eunit/include/eunit.hrl").

-define(TXN_TEST_DB, "./test_txn_db_eunit").

setup_txn_db() ->
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB),
    {ok, TxnDb} = fjall:open_txn(?TXN_TEST_DB),
    TxnDb.

teardown_txn_db(_TxnDb) ->
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB),
    ok.

%% Test: Open transactional database with default options
open_txn_database_test() ->
    setup_txn_db(),
    ?assert(true),
    teardown_txn_db(ok).

%% Test: Open transactional database with options
open_txn_with_options_test() ->
    TxnDb = setup_txn_db(),
    Options = [{cache_size, 1024 * 1024}],
    {ok, _TxnDb2} = fjall:open_txn(?TXN_TEST_DB ++ "_2", Options),
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB ++ "_2"),
    teardown_txn_db(TxnDb).

%% Test: Open keyspace in transactional database
open_txn_keyspace_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"test_keyspace">>),
    ?assert(is_reference(Ks)),
    teardown_txn_db(TxnDb).

%% Test: Write transaction with commit
write_transaction_commit_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx, Ks, <<"key1">>, <<"value1">>),
    ok = fjall:txn_insert(WriteTx, Ks, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx),

    % Verify data was persisted
    {ok, Ks2} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Ks2, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Ks2, <<"key2">>),

    teardown_txn_db(TxnDb).

%% Test: Write transaction with explicit rollback
write_transaction_rollback_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    % Insert initial value
    {ok, WriteTx0} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx0, Ks, <<"key1">>, <<"initial">>),
    ok = fjall:commit_txn(WriteTx0),

    % Start new transaction and rollback
    {ok, WriteTx1} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx1, Ks, <<"key1">>, <<"changed">>),
    ok = fjall:rollback_txn(WriteTx1),

    % Verify original value is still there
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {ok, <<"initial">>} = fjall:read_txn_get(ReadTx, Ks, <<"key1">>),

    teardown_txn_db(TxnDb).

%% Test: Read-your-own-writes semantics
read_your_own_writes_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnDb),

    % Key doesn't exist yet
    {error, not_found} = fjall:txn_get(WriteTx, Ks, <<"new_key">>),

    % Insert in transaction
    ok = fjall:txn_insert(WriteTx, Ks, <<"new_key">>, <<"new_value">>),

    % Now should be visible (RYOW)
    {ok, <<"new_value">>} = fjall:txn_get(WriteTx, Ks, <<"new_key">>),

    ok = fjall:commit_txn(WriteTx),

    teardown_txn_db(TxnDb).

%% Test: Snapshot isolation in read transactions
read_transaction_isolation_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    % Insert initial value
    {ok, WriteTx0} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx0, Ks, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx0),

    % Start read transaction
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Ks, <<"key1">>),

    % Make concurrent write
    {ok, WriteTx1} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx1, Ks, <<"key1">>, <<"value1_updated">>),
    ok = fjall:commit_txn(WriteTx1),

    % Read transaction still sees old value (snapshot isolation)
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Ks, <<"key1">>),

    teardown_txn_db(TxnDb).

%% Test: Multi-keyspace transaction atomicity
multi_keyspace_transaction_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks1} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),
    {ok, Ks2} = fjall:open_txn_keyspace(TxnDb, <<"ks2">>),

    % Write to both keyspaces in one transaction
    {ok, WriteTx} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx, Ks1, <<"key1">>, <<"value1">>),
    ok = fjall:txn_insert(WriteTx, Ks2, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx),

    % Verify both writes were persisted
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Ks1, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Ks2, <<"key2">>),

    teardown_txn_db(TxnDb).

%% Test: Error on operations after commit
transaction_finalized_error_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx, Ks, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx),

    % Try to use transaction after commit
    {error, transaction_already_finalized} = fjall:txn_insert(
        WriteTx, Ks, <<"key2">>, <<"value2">>
    ),

    teardown_txn_db(TxnDb).

%% Test: Transaction remove operation
transaction_remove_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    % Insert and commit
    {ok, WriteTx0} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx0, Ks, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx0),

    % Remove in new transaction
    {ok, WriteTx1} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_remove(WriteTx1, Ks, <<"key1">>),
    ok = fjall:commit_txn(WriteTx1),

    % Verify key is gone
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {error, not_found} = fjall:read_txn_get(ReadTx, Ks, <<"key1">>),

    teardown_txn_db(TxnDb).

%% Test: Multiple transactions on same keyspace
concurrent_transactions_test() ->
    TxnDb = setup_txn_db(),
    {ok, Ks} = fjall:open_txn_keyspace(TxnDb, <<"ks1">>),

    % First transaction
    {ok, WriteTx1} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx1, Ks, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx1),

    % Second transaction
    {ok, WriteTx2} = fjall:begin_write_txn(TxnDb),
    ok = fjall:txn_insert(WriteTx2, Ks, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx2),

    % Verify both writes persisted
    {ok, ReadTx} = fjall:begin_read_txn(TxnDb),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Ks, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Ks, <<"key2">>),

    teardown_txn_db(TxnDb).
