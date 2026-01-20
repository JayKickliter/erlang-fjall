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

iter_test() ->
    DbPath = test_db_path("iter"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    % Insert test data
    ok = fjall_otx_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_otx_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_otx_ks:insert(Ks, <<"c">>, <<"3">>),

    % Test iter/1 with collect
    {ok, Iter1} = fjall_otx_ks:iter(Ks),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Test iter/2 with reverse
    {ok, Iter2} = fjall_otx_ks:iter(Ks, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}, {<<"a">>, <<"1">>}] = Items2,

    ok.

iter_next_test() ->
    DbPath = test_db_path("iter_next"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    ok = fjall_otx_ks:insert(Ks, <<"x">>, <<"1">>),
    ok = fjall_otx_ks:insert(Ks, <<"y">>, <<"2">>),

    {ok, Iter} = fjall_otx_ks:iter(Ks),
    {ok, {<<"x">>, <<"1">>}} = fjall_iter:next(Iter),
    {ok, {<<"y">>, <<"2">>}} = fjall_iter:next(Iter),
    done = fjall_iter:next(Iter),
    % Calling next on exhausted iterator returns done
    done = fjall_iter:next(Iter),

    ok.

iter_take_test() ->
    DbPath = test_db_path("iter_take"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    ok = fjall_otx_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_otx_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_otx_ks:insert(Ks, <<"c">>, <<"3">>),

    {ok, Iter} = fjall_otx_ks:iter(Ks),
    {ok, Items1} = fjall_iter:take(Iter, 2),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}] = Items1,
    {ok, Items2} = fjall_iter:take(Iter, 2),
    [{<<"c">>, <<"3">>}] = Items2,
    {ok, []} = fjall_iter:take(Iter, 2),

    ok.

range_test() ->
    DbPath = test_db_path("range"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    ok = fjall_otx_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_otx_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_otx_ks:insert(Ks, <<"c">>, <<"3">>),
    ok = fjall_otx_ks:insert(Ks, <<"d">>, <<"4">>),

    % Range [b, d) - half-open interval
    {ok, Iter1} = fjall_otx_ks:range(Ks, <<"b">>, <<"d">>),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Range with reverse
    {ok, Iter2} = fjall_otx_ks:range(Ks, <<"b">>, <<"d">>, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}] = Items2,

    ok.

prefix_test() ->
    DbPath = test_db_path("prefix"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    ok = fjall_otx_ks:insert(Ks, <<"user:1">>, <<"alice">>),
    ok = fjall_otx_ks:insert(Ks, <<"user:2">>, <<"bob">>),
    ok = fjall_otx_ks:insert(Ks, <<"order:1">>, <<"pizza">>),

    % Prefix scan
    {ok, Iter1} = fjall_otx_ks:prefix(Ks, <<"user:">>),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"user:1">>, <<"alice">>}, {<<"user:2">>, <<"bob">>}] = Items1,
    done = fjall_iter:next(Iter1),

    % Prefix with reverse
    {ok, Iter2} = fjall_otx_ks:prefix(Ks, <<"user:">>, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"user:2">>, <<"bob">>}, {<<"user:1">>, <<"alice">>}] = Items2,

    ok.

iter_destroy_test() ->
    DbPath = test_db_path("iter_destroy"),
    {ok, Db} = fjall_otx_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_otx_db:keyspace(Db, <<"test">>),

    ok = fjall_otx_ks:insert(Ks, <<"a">>, <<"1">>),

    {ok, Iter} = fjall_otx_ks:iter(Ks),
    ok = fjall_iter:destroy(Iter),
    % After destroy, iterator returns done
    done = fjall_iter:next(Iter),

    ok.

test_db_path(Name) ->
    filename:join(["/tmp", "fjall_otx_test", Name]).
