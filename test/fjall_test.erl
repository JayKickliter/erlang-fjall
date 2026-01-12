-module(fjall_test).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_DB, "./test_db_eunit").

setup() ->
    % Clean up any existing test database
    _ = os:cmd("rm -rf " ++ ?TEST_DB),
    ok.

teardown(_) ->
    % Clean up after tests
    _ = os:cmd("rm -rf " ++ ?TEST_DB),
    ok.

open_database_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB),
    ?assert(is_reference(Database)),
    teardown(ok).

open_database_with_empty_options_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    ?assert(is_reference(Database)),
    teardown(ok).

open_database_with_options_test() ->
    setup(),
    Options = [
        {cache_size, 16777216},
        {worker_threads, 2}
    ],
    {ok, Database} = fjall:open(?TEST_DB, Options),
    ?assert(is_reference(Database)),
    teardown(ok).

open_keyspace_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    {ok, Keyspace} = fjall:open_keyspace(Database, <<"test_keyspace">>),
    ?assert(is_reference(Keyspace)),
    teardown(ok).

insert_and_get_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    {ok, Keyspace} = fjall:open_keyspace(Database, <<"test_keyspace">>),

    Key = <<"hello">>,
    Value = <<"world">>,

    % Insert key-value pair
    ok = fjall:insert(Keyspace, Key, Value),

    % Get the value back
    {ok, RetrievedValue} = fjall:get(Keyspace, Key),
    ?assertEqual(Value, RetrievedValue),
    teardown(ok).

get_missing_key_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    {ok, Keyspace} = fjall:open_keyspace(Database, <<"test_keyspace">>),

    % Try to get a key that doesn't exist
    {error, not_found} = fjall:get(Keyspace, <<"missing">>),
    teardown(ok).

remove_key_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    {ok, Keyspace} = fjall:open_keyspace(Database, <<"test_keyspace">>),

    Key = <<"remove_me">>,
    Value = <<"temporary">>,

    % Insert and then remove
    ok = fjall:insert(Keyspace, Key, Value),
    {ok, Value} = fjall:get(Keyspace, Key),
    ok = fjall:remove(Keyspace, Key),
    {error, not_found} = fjall:get(Keyspace, Key),
    teardown(ok).

multiple_keys_test() ->
    setup(),
    {ok, Database} = fjall:open(?TEST_DB, []),
    {ok, Keyspace} = fjall:open_keyspace(Database, <<"test_keyspace">>),

    % Insert multiple key-value pairs
    ok = fjall:insert(Keyspace, <<"key1">>, <<"value1">>),
    ok = fjall:insert(Keyspace, <<"key2">>, <<"value2">>),
    ok = fjall:insert(Keyspace, <<"key3">>, <<"value3">>),

    % Retrieve them back
    {ok, <<"value1">>} = fjall:get(Keyspace, <<"key1">>),
    {ok, <<"value2">>} = fjall:get(Keyspace, <<"key2">>),
    {ok, <<"value3">>} = fjall:get(Keyspace, <<"key3">>),
    teardown(ok).
