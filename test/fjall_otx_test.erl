-module(fjall_otx_test).

-include_lib("eunit/include/eunit.hrl").

transaction_basic_test() ->
    DbPath = test_db_path("transaction_basic"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    % Create transaction
    {ok, Tx} = fjall_otx_db:write_tx(Db),

    % Insert and read within transaction
    ok = fjall_otx_tx:insert(Tx, Ks, <<"key1">>, <<"value1">>),
    {ok, <<"value1">>} = fjall_otx_tx:get(Tx, Ks, <<"key1">>),

    % Commit
    ok = fjall_otx_tx:commit(Tx),

    % Verify with snapshot
    {ok, Snapshot} = fjall_otx_db:snapshot(Db),
    {ok, <<"value1">>} = fjall_snapshot:get(Snapshot, Ks, <<"key1">>),

    ok.

transaction_rollback_test() ->
    DbPath = test_db_path("transaction_rollback"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    % Create transaction
    {ok, Tx} = fjall_otx_db:write_tx(Db),

    % Insert
    ok = fjall_otx_tx:insert(Tx, Ks, <<"key1">>, <<"value1">>),

    % Rollback
    ok = fjall_otx_tx:rollback(Tx),

    % Verify data was not committed
    {ok, Snapshot} = fjall_otx_db:snapshot(Db),
    not_found = fjall_snapshot:get(Snapshot, Ks, <<"key1">>),

    ok.

snapshot_test() ->
    DbPath = test_db_path("snapshot"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    % Insert initial data
    {ok, Tx1} = fjall_otx_db:write_tx(Db),
    ok = fjall_otx_tx:insert(Tx1, Ks, <<"key1">>, <<"value1">>),
    ok = fjall_otx_tx:commit(Tx1),

    % Create snapshot
    {ok, Snapshot} = fjall_otx_db:snapshot(Db),
    {ok, <<"value1">>} = fjall_snapshot:get(Snapshot, Ks, <<"key1">>),

    % Modify data after snapshot
    {ok, Tx2} = fjall_otx_db:write_tx(Db),
    ok = fjall_otx_tx:insert(Tx2, Ks, <<"key1">>, <<"value2">>),
    ok = fjall_otx_tx:commit(Tx2),

    % Snapshot still sees old value
    {ok, <<"value1">>} = fjall_snapshot:get(Snapshot, Ks, <<"key1">>),

    ok.

test_db_path(Name) ->
    filename:join(["/tmp", "fjall_otx_test", Name]).
