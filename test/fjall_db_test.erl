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

iter_collect_test() ->
    DbPath = test_db_path("iter_collect"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    {ok, Iter} = fjall:iter(Ks, forward),
    {ok, Items1} = fjall:collect(Iter, 2),
    [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}] = Items1,
    {ok, Items2} = fjall:collect(Iter, 2),
    [{<<"c">>, <<"3">>}] = Items2,
    {ok, []} = fjall:collect(Iter, 2),

    ok.

iter_collect_keys_test() ->
    DbPath = test_db_path("iter_collect_keys"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    % collect_keys/1 - all keys
    {ok, Iter1} = fjall:iter(Ks, forward),
    {ok, Keys1} = fjall:collect_keys(Iter1),
    [<<"a">>, <<"b">>, <<"c">>] = Keys1,

    % collect_keys/2 - with limit
    {ok, Iter2} = fjall:iter(Ks, forward),
    {ok, Keys2} = fjall:collect_keys(Iter2, 2),
    [<<"a">>, <<"b">>] = Keys2,
    {ok, Keys3} = fjall:collect_keys(Iter2, 2),
    [<<"c">>] = Keys3,
    {ok, []} = fjall:collect_keys(Iter2, 2),

    % collect_keys with reverse
    {ok, Iter3} = fjall:iter(Ks, reverse),
    {ok, Keys4} = fjall:collect_keys(Iter3),
    [<<"c">>, <<"b">>, <<"a">>] = Keys4,

    ok.

iter_collect_values_test() ->
    DbPath = test_db_path("iter_collect_values"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    % collect_values/1 - all values
    {ok, Iter1} = fjall:iter(Ks, forward),
    {ok, Values1} = fjall:collect_values(Iter1),
    [<<"1">>, <<"2">>, <<"3">>] = Values1,

    % collect_values/2 - with limit
    {ok, Iter2} = fjall:iter(Ks, forward),
    {ok, Values2} = fjall:collect_values(Iter2, 2),
    [<<"1">>, <<"2">>] = Values2,
    {ok, Values3} = fjall:collect_values(Iter2, 2),
    [<<"3">>] = Values3,
    {ok, []} = fjall:collect_values(Iter2, 2),

    % collect_values with reverse
    {ok, Iter3} = fjall:iter(Ks, reverse),
    {ok, Values4} = fjall:collect_values(Iter3),
    [<<"3">>, <<"2">>, <<"1">>] = Values4,

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
    {ok, Iter1} = fjall:iter(Ks, forward, exclusive, {<<"b">>, <<"d">>}),
    {ok, Items1} = fjall:collect(Iter1),
    [{<<"b">>, <<"2">>}, {<<"c">>, <<"3">>}] = Items1,

    % Exclusive range with reverse
    {ok, Iter2} = fjall:iter(Ks, reverse, exclusive, {<<"b">>, <<"d">>}),
    {ok, Items2} = fjall:collect(Iter2),
    [{<<"c">>, <<"3">>}, {<<"b">>, <<"2">>}] = Items2,

    % Inclusive range [b, d] - closed interval
    {ok, Iter3} = fjall:iter(Ks, forward, inclusive, {<<"b">>, <<"d">>}),
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
    {ok, Iter1} = fjall:iter(Ks, forward, <<"user:">>),
    {ok, Items1} = fjall:collect(Iter1),
    [{<<"user:1">>, <<"alice">>}, {<<"user:2">>, <<"bob">>}] = Items1,

    % Prefix with reverse
    {ok, Iter2} = fjall:iter(Ks, reverse, <<"user:">>),
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

clear_test() ->
    DbPath = test_db_path("clear"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    % Insert test data
    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),

    % Verify data exists
    {ok, <<"1">>} = fjall:get(Ks, <<"a">>),
    {ok, <<"2">>} = fjall:get(Ks, <<"b">>),
    {ok, <<"3">>} = fjall:get(Ks, <<"c">>),

    % Clear the keyspace
    ok = fjall:clear(Ks),

    % Verify keyspace is empty
    not_found = fjall:get(Ks, <<"a">>),
    not_found = fjall:get(Ks, <<"b">>),
    not_found = fjall:get(Ks, <<"c">>),
    {error, not_found} = fjall:first_key_value(Ks),

    ok.

keyspace_info_test() ->
    DbPath = test_db_path("keyspace_info"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    % Empty keyspace
    {error, not_found} = fjall:first_key_value(Ks),
    {error, not_found} = fjall:last_key_value(Ks),

    % Insert test data
    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"22">>),
    ok = fjall:insert(Ks, <<"c">>, <<"333">>),

    % contains_key
    {ok, true} = fjall:contains_key(Ks, <<"a">>),
    {ok, false} = fjall:contains_key(Ks, <<"nonexistent">>),

    % size_of
    {ok, 1} = fjall:size_of(Ks, <<"a">>),
    {ok, 2} = fjall:size_of(Ks, <<"b">>),
    {ok, 3} = fjall:size_of(Ks, <<"c">>),
    {error, not_found} = fjall:size_of(Ks, <<"nonexistent">>),

    % approximate_len (should be around 3)
    {ok, Len} = fjall:approximate_len(Ks),
    true = Len >= 0,

    % first_key_value
    {ok, {<<"a">>, <<"1">>}} = fjall:first_key_value(Ks),

    % last_key_value
    {ok, {<<"c">>, <<"333">>}} = fjall:last_key_value(Ks),

    % path
    {ok, Path} = fjall:path(Ks),
    true = is_binary(Path),

    ok.

len_test() ->
    DbPath = test_db_path("len"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),

    % Empty keyspace
    {ok, 0} = fjall:len(Ks),

    % Insert some data
    ok = fjall:insert(Ks, <<"a">>, <<"1">>),
    ok = fjall:insert(Ks, <<"b">>, <<"2">>),
    ok = fjall:insert(Ks, <<"c">>, <<"3">>),
    {ok, 3} = fjall:len(Ks),

    % Remove one
    ok = fjall:remove(Ks, <<"b">>),
    {ok, 2} = fjall:len(Ks),

    ok.

%% Opening the same keyspace name twice reuses the same Arc,
%% so both handles should work.
db_duplicate_keyspace_test() ->
    DbPath = test_db_path("dup_ks"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks1} = fjall:keyspace(Db, <<"test">>),
    ok = fjall:insert(Ks1, <<"key">>, <<"value">>),
    {ok, <<"value">>} = fjall:get(Ks1, <<"key">>),
    %% Open the same keyspace again, the old handle to the same
    %% keyspace should still work since we're reusing the same Arc on
    %% the rust side.
    {ok, Ks2} = fjall:keyspace(Db, <<"test">>),
    ?assertMatch({ok, <<"value">>}, fjall:get(Ks1, <<"key">>)),
    %% New handle works
    {ok, <<"value">>} = fjall:get(Ks2, <<"key">>),
    ok.

%% After GC'ing the DB, the lock should be released and
%% the keyspace should be invalidated.
db_gc_keyspace_releases_lock_test() ->
    DbPath = test_db_path("gc_lock"),
    Ks = spawn_open_db(DbPath, fun(Db) ->
        {ok, Ks} = fjall:keyspace(Db, <<"test">>),
        ok = fjall:insert(Ks, <<"key1">>, <<"value1">>),
        ok = fjall:insert(Ks, <<"key2">>, <<"value2">>),
        Ks
    end),
    %% Lock released — reopen should succeed
    {ok, _Db2} = fjall:open(DbPath),
    %% Keyspace invalidated
    ?assertMatch({error, db_closed}, fjall:get(Ks, <<"key1">>)),
    ok.

%% After GC'ing both DB and keyspace, a surviving iterator
%% should not hold the lock, and the DB should be reopenable.
db_gc_iterator_does_not_hold_lock_test() ->
    DbPath = test_db_path("gc_iter_lock"),
    Iter = spawn_open_db(DbPath, fun(Db) ->
        {ok, Ks} = fjall:keyspace(Db, <<"test">>),
        ok = fjall:insert(Ks, <<"key1">>, <<"value1">>),
        ok = fjall:insert(Ks, <<"key2">>, <<"value2">>),
        {ok, Iter} = fjall:iter(Ks, forward),
        Iter
    end),
    %% Lock released — reopen should succeed
    {ok, _} = fjall:open(DbPath),
    %% Iterator still works (self-contained snapshot)
    {ok, {<<"key1">>, <<"value1">>}} = fjall:next(Iter),
    {ok, {<<"key2">>, <<"value2">>}} = fjall:next(Iter),
    ok.

db_close_test() ->
    DbPath = test_db_path("close"),
    {ok, Db} = fjall:open(DbPath, [{temporary, true}]),
    {ok, Ks} = fjall:keyspace(Db, <<"test">>),
    ok = fjall:insert(Ks, <<"key">>, <<"value">>),
    {ok, <<"value">>} = fjall:get(Ks, <<"key">>),
    %% Create iterator before close
    {ok, Iter} = fjall:iter(Ks, forward),
    ok = fjall:close(Db),
    %% Iterator created before close still works (self-contained snapshot)
    {ok, {<<"key">>, <<"value">>}} = fjall:next(Iter),
    done = fjall:next(Iter),
    %% DB operations fail after close
    ?assertMatch({error, db_closed}, fjall:keyspace(Db, <<"test">>)),
    ?assertMatch({error, db_closed}, fjall:batch(Db)),
    ?assertMatch({error, db_closed}, fjall:persist(Db, sync_all)),
    %% Keyspace operations fail after close
    ?assertMatch({error, db_closed}, fjall:get(Ks, <<"key">>)),
    ?assertMatch({error, db_closed}, fjall:insert(Ks, <<"k">>, <<"v">>)),
    ?assertMatch({error, db_closed}, fjall:remove(Ks, <<"key">>)),
    ?assertMatch({error, db_closed}, fjall:contains_key(Ks, <<"key">>)),
    %% Closing again is idempotent
    ok = fjall:close(Db),
    ok.

%% Opens a DB in a spawned process, runs Fun(Db), and waits for
%% the process to exit. This gaurantees the Db ref is freed
%% deterministcally when the process dies.
spawn_open_db(DbPath, Fun) ->
    Self = self(),
    Pid = spawn(fun() ->
        {ok, Db} = fjall:open(DbPath),
        Result = Fun(Db),
        Self ! {result, Result}
    end),
    MRef = monitor(process, Pid),
    Result =
        receive
            {result, R} -> R
        end,
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    end,
    Result.

test_db_path(Name) ->
    Rand = binary_to_list(
        base64:encode(crypto:strong_rand_bytes(16), #{mode => urlsafe, padding => false})
    ),
    filename:join(["/tmp", "fjall_db_test", Name ++ "_" ++ Rand]).
