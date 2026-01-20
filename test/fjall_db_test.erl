-module(fjall_db_test).

-include_lib("eunit/include/eunit.hrl").

basic_operations_test() ->
    DbPath = test_db_path("basic_operations"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    % Insert
    ok = fjall_ks:insert(Ks, <<"key1">>, <<"value1">>),

    % Get
    {ok, <<"value1">>} = fjall_ks:get(Ks, <<"key1">>),

    % Not found
    not_found = fjall_ks:get(Ks, <<"nonexistent">>),

    % Remove
    ok = fjall_ks:remove(Ks, <<"key1">>),
    not_found = fjall_ks:get(Ks, <<"key1">>),

    ok.

write_batch_test() ->
    DbPath = test_db_path("write_batch"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks1} = fjall_db:keyspace(Db, <<"ks1">>),
    {ok, Ks2} = fjall_db:keyspace(Db, <<"ks2">>),

    % Create batch
    {ok, Batch} = fjall_db:batch(Db),

    % Add operations
    ok = fjall_wb:insert(Batch, Ks1, <<"key1">>, <<"value1">>),
    ok = fjall_wb:insert(Batch, Ks2, <<"key2">>, <<"value2">>),

    % Check batch is not empty
    false = fjall_wb:is_empty(Batch),
    2 = fjall_wb:len(Batch),

    % Commit
    ok = fjall_wb:commit(Batch),

    % Verify data
    {ok, <<"value1">>} = fjall_ks:get(Ks1, <<"key1">>),
    {ok, <<"value2">>} = fjall_ks:get(Ks2, <<"key2">>),

    ok.

iter_test() ->
    DbPath = test_db_path("iter"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    % Insert test data
    ok = fjall_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_ks:insert(Ks, <<"c">>, <<"3">>),

    % Test iter/1 with collect
    {ok, Iter1} = fjall_ks:iter(Ks),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Test iter/2 with reverse
    {ok, Iter2} = fjall_ks:iter(Ks, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}, {<<"a">>, <<"1">>}] = Items2,

    ok.

iter_next_test() ->
    DbPath = test_db_path("iter_next"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    ok = fjall_ks:insert(Ks, <<"x">>, <<"1">>),
    ok = fjall_ks:insert(Ks, <<"y">>, <<"2">>),

    {ok, Iter} = fjall_ks:iter(Ks),
    {ok, {<<"x">>, <<"1">>}} = fjall_iter:next(Iter),
    {ok, {<<"y">>, <<"2">>}} = fjall_iter:next(Iter),
    done = fjall_iter:next(Iter),
    % Calling next on exhausted iterator returns done
    done = fjall_iter:next(Iter),

    ok.

iter_take_test() ->
    DbPath = test_db_path("iter_take"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    ok = fjall_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_ks:insert(Ks, <<"c">>, <<"3">>),

    {ok, Iter} = fjall_ks:iter(Ks),
    {ok, Items1} = fjall_iter:take(Iter, 2),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}] = Items1,
    {ok, Items2} = fjall_iter:take(Iter, 2),
    [{<<"c">>, <<"3">>}] = Items2,
    {ok, []} = fjall_iter:take(Iter, 2),

    ok.

range_test() ->
    DbPath = test_db_path("range"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    ok = fjall_ks:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall_ks:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall_ks:insert(Ks, <<"c">>, <<"3">>),
    ok = fjall_ks:insert(Ks, <<"d">>, <<"4">>),

    % Range [b, d) - half-open interval
    {ok, Iter1} = fjall_ks:range(Ks, <<"b">>, <<"d">>),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Range with reverse
    {ok, Iter2} = fjall_ks:range(Ks, <<"b">>, <<"d">>, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}] = Items2,

    ok.

prefix_test() ->
    DbPath = test_db_path("prefix"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    ok = fjall_ks:insert(Ks, <<"user:1">>, <<"alice">>),
    ok = fjall_ks:insert(Ks, <<"user:2">>, <<"bob">>),
    ok = fjall_ks:insert(Ks, <<"order:1">>, <<"pizza">>),

    % Prefix scan
    {ok, Iter1} = fjall_ks:prefix(Ks, <<"user:">>),
    {ok, Items1} = fjall_iter:collect(Iter1),
    [{<<"user:1">>, <<"alice">>}, {<<"user:2">>, <<"bob">>}] = Items1,

    % Prefix with reverse
    {ok, Iter2} = fjall_ks:prefix(Ks, <<"user:">>, [reverse]),
    {ok, Items2} = fjall_iter:collect(Iter2),
    [{<<"user:2">>, <<"bob">>}, {<<"user:1">>, <<"alice">>}] = Items2,

    ok.

iter_destroy_test() ->
    DbPath = test_db_path("iter_destroy"),
    {ok, Db} = fjall_db:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall_db:keyspace(Db, <<"test">>),

    ok = fjall_ks:insert(Ks, <<"a">>, <<"1">>),

    {ok, Iter} = fjall_ks:iter(Ks),
    ok = fjall_iter:destroy(Iter),
    % After destroy, iterator returns done
    done = fjall_iter:next(Iter),

    ok.

test_db_path(Name) ->
    filename:join(["/tmp", "fjall_test", Name]).
