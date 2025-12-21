-module(fjall_txn_test).
-include_lib("eunit/include/eunit.hrl").

-define(TXN_TEST_DB, "./test_txn_db_eunit").

setup_txn_db() ->
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB),
    {ok, TxnKs} = fjall:open_txn(?TXN_TEST_DB),
    TxnKs.

teardown_txn_db(_TxnKs) ->
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB),
    ok.

%% Test: Open transactional keyspace with default options
open_txn_keyspace_test() ->
    setup_txn_db(),
    ?assert(true),
    teardown_txn_db(ok).

%% Test: Open transactional keyspace with options
open_txn_with_options_test() ->
    TxnKs = setup_txn_db(),
    Options = [{cache_size, 1024 * 1024}],
    {ok, _TxnKs2} = fjall:open_txn(?TXN_TEST_DB ++ "_2", Options),
    _ = os:cmd("rm -rf " ++ ?TXN_TEST_DB ++ "_2"),
    teardown_txn_db(TxnKs).

%% Test: Open partition in transactional keyspace
open_txn_partition_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"test_part">>),
    ?assert(is_reference(Part)),
    teardown_txn_db(TxnKs).

%% Test: Write transaction with commit
write_transaction_commit_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx, Part, <<"key1">>, <<"value1">>),
    ok = fjall:txn_insert(WriteTx, Part, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx),

    % Verify data was persisted
    {ok, Part2} = fjall:open_txn_partition(TxnKs, <<"part1">>),
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Part2, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Part2, <<"key2">>),

    teardown_txn_db(TxnKs).

%% Test: Write transaction with explicit rollback
write_transaction_rollback_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    % Insert initial value
    {ok, WriteTx0} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx0, Part, <<"key1">>, <<"initial">>),
    ok = fjall:commit_txn(WriteTx0),

    % Start new transaction and rollback
    {ok, WriteTx1} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx1, Part, <<"key1">>, <<"changed">>),
    ok = fjall:rollback_txn(WriteTx1),

    % Verify original value is still there
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {ok, <<"initial">>} = fjall:read_txn_get(ReadTx, Part, <<"key1">>),

    teardown_txn_db(TxnKs).

%% Test: Read-your-own-writes semantics
read_your_own_writes_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnKs),

    % Key doesn't exist yet
    {error, not_found} = fjall:txn_get(WriteTx, Part, <<"new_key">>),

    % Insert in transaction
    ok = fjall:txn_insert(WriteTx, Part, <<"new_key">>, <<"new_value">>),

    % Now should be visible (RYOW)
    {ok, <<"new_value">>} = fjall:txn_get(WriteTx, Part, <<"new_key">>),

    ok = fjall:commit_txn(WriteTx),

    teardown_txn_db(TxnKs).

%% Test: Snapshot isolation in read transactions
read_transaction_isolation_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    % Insert initial value
    {ok, WriteTx0} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx0, Part, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx0),

    % Start read transaction
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Part, <<"key1">>),

    % Make concurrent write
    {ok, WriteTx1} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx1, Part, <<"key1">>, <<"value1_updated">>),
    ok = fjall:commit_txn(WriteTx1),

    % Read transaction still sees old value (snapshot isolation)
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Part, <<"key1">>),

    teardown_txn_db(TxnKs).

%% Test: Multi-partition transaction atomicity
multi_partition_transaction_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part1} = fjall:open_txn_partition(TxnKs, <<"part1">>),
    {ok, Part2} = fjall:open_txn_partition(TxnKs, <<"part2">>),

    % Write to both partitions in one transaction
    {ok, WriteTx} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx, Part1, <<"key1">>, <<"value1">>),
    ok = fjall:txn_insert(WriteTx, Part2, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx),

    % Verify both writes were persisted
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Part1, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Part2, <<"key2">>),

    teardown_txn_db(TxnKs).

%% Test: Error on operations after commit
transaction_finalized_error_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    {ok, WriteTx} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx, Part, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx),

    % Try to use transaction after commit
    {error, transaction_already_finalized} = fjall:txn_insert(
        WriteTx, Part, <<"key2">>, <<"value2">>
    ),

    teardown_txn_db(TxnKs).

%% Test: Transaction remove operation
transaction_remove_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    % Insert and commit
    {ok, WriteTx0} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx0, Part, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx0),

    % Remove in new transaction
    {ok, WriteTx1} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_remove(WriteTx1, Part, <<"key1">>),
    ok = fjall:commit_txn(WriteTx1),

    % Verify key is gone
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {error, not_found} = fjall:read_txn_get(ReadTx, Part, <<"key1">>),

    teardown_txn_db(TxnKs).

%% Test: Multiple transactions on same partition
concurrent_transactions_test() ->
    TxnKs = setup_txn_db(),
    {ok, Part} = fjall:open_txn_partition(TxnKs, <<"part1">>),

    % First transaction
    {ok, WriteTx1} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx1, Part, <<"key1">>, <<"value1">>),
    ok = fjall:commit_txn(WriteTx1),

    % Second transaction
    {ok, WriteTx2} = fjall:begin_write_txn(TxnKs),
    ok = fjall:txn_insert(WriteTx2, Part, <<"key2">>, <<"value2">>),
    ok = fjall:commit_txn(WriteTx2),

    % Verify both writes persisted
    {ok, ReadTx} = fjall:begin_read_txn(TxnKs),
    {ok, <<"value1">>} = fjall:read_txn_get(ReadTx, Part, <<"key1">>),
    {ok, <<"value2">>} = fjall:read_txn_get(ReadTx, Part, <<"key2">>),

    teardown_txn_db(TxnKs).
