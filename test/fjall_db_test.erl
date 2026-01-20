-module(fjall_db_test).

-include_lib("eunit/include/eunit.hrl").

basic_operations_test() ->
    DbPath = test_db_path("basic_operations"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    % Insert
    ok = fjall:insert(Ks, <<"key1">>, <<"value1">>),

    % Get
    {ok, <<"value1">>} = fjall:get(Ks, <<"key1">>),

    % Not found
    not_found = fjall:get(Ks, <<"nonexistent">>),

    % Remove
    ok = fjall:remove(Ks, <<"key1">>),
    not_found = fjall:get(Ks, <<"key1">>),

    ok.

write_batch_test() ->
    DbPath = test_db_path("write_batch"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks1} = fjall:keyspace(Db, <<"ks1">>),
    {ok, Ks2} = fjall:keyspace(Db, <<"ks2">>),

    % Create batch
    {ok, Batch} = fjall:batch(Db),

    % Add operations
    ok = fjall:insert(Batch, Ks1, <<"key1">>, <<"value1">>),
    ok = fjall:insert(Batch, Ks2, <<"key2">>, <<"value2">>),

    % Check batch is not empty
    false = fjall:is_empty(Batch),
    2 = fjall:len(Batch),

    % Commit
    ok = fjall:commit(Batch),

    % Verify data
    {ok, <<"value1">>} = fjall:get(Ks1, <<"key1">>),
    {ok, <<"value2">>} = fjall:get(Ks2, <<"key2">>),

    ok.

iter_test() ->
    DbPath = test_db_path("iter"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    % Insert test data
    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    % Test iter/2 with forward
    {ok, Iter1} = fjall:iter(Ks, forward),
    {ok, Items1} = fjall:collect(Iter1),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Test iter/2 with reverse
    {ok, Iter2} = fjall:iter(Ks, reverse),
    {ok, Items2} = fjall:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}, {<<"a">>, <<"1">>}] = Items2,

    ok.

iter_next_test() ->
    DbPath = test_db_path("iter_next"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"x">>, <<"1">>),
    ok = fjall:insert(Ks, <<"y">>, <<"2">>),

    {ok, Iter} = fjall:iter(Ks, forward),
    {ok, {<<"x">>, <<"1">>}} = fjall:next(Iter),
    {ok, {<<"y">>, <<"2">>}} = fjall:next(Iter),
    done = fjall:next(Iter),
    % Calling next on exhausted iterator returns done
    done = fjall:next(Iter),

    ok.

iter_take_test() ->
    DbPath = test_db_path("iter_take"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    {ok, Iter} = fjall:iter(Ks, forward),
    {ok, Items1} = fjall:take(Iter, 2),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}] = Items1,
    {ok, Items2} = fjall:take(Iter, 2),
    [{<<"c">>, <<"3">>}] = Items2,
    {ok, []} = fjall:take(Iter, 2),

    ok.

range_test() ->
    DbPath = test_db_path("range"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),
    ok = fjall:insert(Ks, <<"d">>, <<"4">>),

    % Exclusive range [b, d) - half-open interval
    {ok, Iter1} = fjall:range(Ks, <<"b">>, <<"d">>, forward, exclusive),
    {ok, Items1} = fjall:collect(Iter1),
    [{<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Exclusive range with reverse
    {ok, Iter2} = fjall:range(Ks, <<"b">>, <<"d">>, reverse, exclusive),
    {ok, Items2} = fjall:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}] = Items2,

    % Inclusive range [b, d] - closed interval
    {ok, Iter3} = fjall:range(Ks, <<"b">>, <<"d">>, forward, inclusive),
    {ok, Items3} = fjall:collect(Iter3),
    [{<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}, {<<"d">>, <<"4">>}] = Items3,

    ok.

prefix_test() ->
    DbPath = test_db_path("prefix"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"user:1">>, <<"alice">>),
    ok = fjall:insert(Ks, <<"user:2">>, <<"bob">>),
    ok = fjall:insert(Ks, <<"order:1">>, <<"pizza">>),

    % Prefix scan
    {ok, Iter1} = fjall:prefix(Ks, <<"user:">>, forward),
    {ok, Items1} = fjall:collect(Iter1),
    [{<<"user:1">>, <<"alice">>}, {<<"user:2">>, <<"bob">>}] = Items1,

    % Prefix with reverse
    {ok, Iter2} = fjall:prefix(Ks, <<"user:">>, reverse),
    {ok, Items2} = fjall:collect(Iter2),
    [{<<"user:2">>, <<"bob">>}, {<<"user:1">>, <<"alice">>}] = Items2,

    ok.

iter_destroy_test() ->
    DbPath = test_db_path("iter_destroy"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),

    {ok, Iter} = fjall:iter(Ks, forward),
    ok = fjall:destroy(Iter),
    % After destroy, iterator returns done
    done = fjall:next(Iter),

    ok.

test_db_path(Name) ->
    filename:join(["/tmp", "fjall_test", Name]).
