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

test_db_path(Name) ->
    filename:join(["/tmp", "fjall_test", Name]).
