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

open_keyspace_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB),
    ?assert(is_reference(Keyspace)),
    teardown(ok).

open_keyspace_with_empty_options_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    ?assert(is_reference(Keyspace)),
    teardown(ok).

open_keyspace_with_options_test() ->
    setup(),
    Options = [
        {cache_size, 16777216},
        {max_write_buffer_size, 67108864}
    ],
    {ok, Keyspace} = fjall:open(?TEST_DB, Options),
    ?assert(is_reference(Keyspace)),
    teardown(ok).

open_partition_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    {ok, Partition} = fjall:open_partition(Keyspace, <<"test_partition">>),
    ?assert(is_reference(Partition)),
    teardown(ok).

insert_and_get_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    {ok, Partition} = fjall:open_partition(Keyspace, <<"test_partition">>),

    Key = <<"hello">>,
    Value = <<"world">>,

    % Insert key-value pair
    ok = fjall:insert(Partition, Key, Value),

    % Get the value back
    {ok, RetrievedValue} = fjall:get(Partition, Key),
    ?assertEqual(Value, RetrievedValue),
    teardown(ok).

get_missing_key_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    {ok, Partition} = fjall:open_partition(Keyspace, <<"test_partition">>),

    % Try to get a key that doesn't exist
    {error, not_found} = fjall:get(Partition, <<"missing">>),
    teardown(ok).

remove_key_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    {ok, Partition} = fjall:open_partition(Keyspace, <<"test_partition">>),

    Key = <<"remove_me">>,
    Value = <<"temporary">>,

    % Insert and then remove
    ok = fjall:insert(Partition, Key, Value),
    {ok, Value} = fjall:get(Partition, Key),
    ok = fjall:remove(Partition, Key),
    {error, not_found} = fjall:get(Partition, Key),
    teardown(ok).

multiple_keys_test() ->
    setup(),
    {ok, Keyspace} = fjall:open(?TEST_DB, []),
    {ok, Partition} = fjall:open_partition(Keyspace, <<"test_partition">>),

    % Insert multiple key-value pairs
    ok = fjall:insert(Partition, <<"key1">>, <<"value1">>),
    ok = fjall:insert(Partition, <<"key2">>, <<"value2">>),
    ok = fjall:insert(Partition, <<"key3">>, <<"value3">>),

    % Retrieve them back
    {ok, <<"value1">>} = fjall:get(Partition, <<"key1">>),
    {ok, <<"value2">>} = fjall:get(Partition, <<"key2">>),
    {ok, <<"value3">>} = fjall:get(Partition, <<"key3">>),
    teardown(ok).
