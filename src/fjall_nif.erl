-module(fjall_nif).

-moduledoc false.

-on_load(load/0).

-export([
    % fjall_db NIFs
    db_batch/1,
    db_keyspace/3,
    db_open/2,
    db_persist/2,

    % fjall_ks NIFs
    ks_approximate_len/1,
    ks_contains_key/2,
    ks_disk_space/1,
    ks_first_key_value/1,
    ks_get/2,
    ks_insert/3,
    ks_iter/2,
    ks_last_key_value/1,
    ks_path/1,
    ks_prefix/3,
    ks_range/5,
    ks_remove/2,
    ks_size_of/2,

    % fjall_wb NIFs
    wb_commit/1,
    wb_insert/4,
    wb_is_empty/1,
    wb_len/1,
    wb_remove/3,

    % fjall_otx_db NIFs
    otx_db_keyspace/3,
    otx_db_open/2,
    otx_db_persist/2,
    otx_db_snapshot/1,
    otx_db_write_tx/1,

    % fjall_otx_ks NIFs
    otx_ks_approximate_len/1,
    otx_ks_contains_key/2,
    otx_ks_first_key_value/1,
    otx_ks_get/2,
    otx_ks_insert/3,
    otx_ks_iter/2,
    otx_ks_last_key_value/1,
    otx_ks_path/1,
    otx_ks_prefix/3,
    otx_ks_range/5,
    otx_ks_remove/2,
    otx_ks_size_of/2,
    otx_ks_take/2,

    % fjall_otx_tx NIFs
    otx_tx_commit/1,
    otx_tx_get/3,
    otx_tx_insert/4,
    otx_tx_remove/3,
    otx_tx_rollback/1,

    % fjall_snapshot NIFs
    snapshot_get/3,

    % fjall_iter NIFs
    iter_collect/1,
    iter_collect/2,
    iter_collect_keys/1,
    iter_collect_keys/2,
    iter_collect_values/1,
    iter_collect_values/2,
    iter_destroy/1,
    iter_next/1
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF Stubs                                                              %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% fjall_db NIFs
-spec db_open(Path :: binary(), Options :: list()) -> fjall:result(fjall_db:db()).
db_open(_Path, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec db_keyspace(Db :: fjall_db:db(), Name :: binary(), Options :: list()) ->
    fjall:result(fjall_ks:ks()).
db_keyspace(_Db, _Name, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec db_batch(Db :: fjall_db:db()) -> fjall:result(fjall_wb:wb()).
db_batch(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec db_persist(Db :: fjall_db:db(), Mode :: fjall:persist_mode()) -> fjall:result().
db_persist(_Db, _Mode) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_ks NIFs
-spec ks_get(Ks :: fjall_ks:ks(), Key :: binary()) -> fjall:result(binary()).
ks_get(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_insert(Ks :: fjall_ks:ks(), Key :: binary(), Value :: binary()) -> fjall:result().
ks_insert(_Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_remove(Ks :: fjall_ks:ks(), Key :: binary()) -> fjall:result().
ks_remove(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_disk_space(Ks :: fjall_ks:ks()) -> non_neg_integer().
ks_disk_space(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_contains_key(Ks :: fjall_ks:ks(), Key :: binary()) -> fjall:result(boolean()).
ks_contains_key(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_size_of(Ks :: fjall_ks:ks(), Key :: binary()) -> fjall:result(non_neg_integer()).
ks_size_of(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_approximate_len(Ks :: fjall_ks:ks()) -> non_neg_integer().
ks_approximate_len(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_first_key_value(Ks :: fjall_ks:ks()) -> fjall:result({binary(), binary()}).
ks_first_key_value(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_last_key_value(Ks :: fjall_ks:ks()) -> fjall:result({binary(), binary()}).
ks_last_key_value(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_path(Ks :: fjall_ks:ks()) -> binary().
ks_path(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_wb NIFs
-spec wb_insert(Batch :: fjall_wb:wb(), Ks :: fjall_ks:ks(), Key :: binary(), Value :: binary()) ->
    fjall:result().
wb_insert(_Batch, _Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec wb_remove(Batch :: fjall_wb:wb(), Ks :: fjall_ks:ks(), Key :: binary()) -> fjall:result().
wb_remove(_Batch, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec wb_commit(Batch :: fjall_wb:wb()) -> fjall:result().
wb_commit(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec wb_len(Batch :: fjall_wb:wb()) -> non_neg_integer().
wb_len(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec wb_is_empty(Batch :: fjall_wb:wb()) -> boolean().
wb_is_empty(_Batch) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_otx_db NIFs
-spec otx_db_open(Path :: binary(), Options :: list()) -> fjall:result(fjall_otx_db:otx_db()).
otx_db_open(_Path, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_db_keyspace(Db :: fjall_otx_db:otx_db(), Name :: binary(), Options :: list()) ->
    fjall:result(fjall_otx_ks:otx_ks()).
otx_db_keyspace(_Db, _Name, _Options) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_db_write_tx(Db :: fjall_otx_db:otx_db()) -> fjall:result(fjall_otx_tx:write_tx()).
otx_db_write_tx(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_db_snapshot(Db :: fjall_otx_db:otx_db()) -> fjall:result(fjall_snapshot:snapshot()).
otx_db_snapshot(_Db) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_db_persist(Db :: fjall_otx_db:otx_db(), Mode :: fjall:persist_mode()) -> fjall:result().
otx_db_persist(_Db, _Mode) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_otx_tx NIFs
-spec otx_tx_insert(
    Tx :: fjall_otx_tx:write_tx(), Ks :: fjall_otx_ks:otx_ks(), Key :: binary(), Value :: binary()
) -> fjall:result().
otx_tx_insert(_Tx, _Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_tx_get(Tx :: fjall_otx_tx:write_tx(), Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) ->
    fjall:result(binary()).
otx_tx_get(_Tx, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_tx_remove(Tx :: fjall_otx_tx:write_tx(), Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) ->
    fjall:result().
otx_tx_remove(_Tx, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_tx_commit(Tx :: fjall_otx_tx:write_tx()) -> fjall:result().
otx_tx_commit(_Tx) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_tx_rollback(Tx :: fjall_otx_tx:write_tx()) -> fjall:result().
otx_tx_rollback(_Tx) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_snapshot NIFs
-spec snapshot_get(
    Snapshot :: fjall_snapshot:snapshot(), Ks :: fjall_otx_ks:otx_ks(), Key :: binary()
) -> fjall:result(binary()).
snapshot_get(_Snapshot, _Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_otx_ks NIFs
-spec otx_ks_insert(Ks :: fjall_otx_ks:otx_ks(), Key :: binary(), Value :: binary()) ->
    fjall:result().
otx_ks_insert(_Ks, _Key, _Value) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_get(Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) -> fjall:result(binary()).
otx_ks_get(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_remove(Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) -> fjall:result().
otx_ks_remove(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_take(Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) -> fjall:result(binary()).
otx_ks_take(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_contains_key(Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) -> fjall:result(boolean()).
otx_ks_contains_key(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_size_of(Ks :: fjall_otx_ks:otx_ks(), Key :: binary()) ->
    fjall:result(non_neg_integer()).
otx_ks_size_of(_Ks, _Key) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_approximate_len(Ks :: fjall_otx_ks:otx_ks()) -> non_neg_integer().
otx_ks_approximate_len(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_first_key_value(Ks :: fjall_otx_ks:otx_ks()) -> fjall:result({binary(), binary()}).
otx_ks_first_key_value(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_last_key_value(Ks :: fjall_otx_ks:otx_ks()) -> fjall:result({binary(), binary()}).
otx_ks_last_key_value(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_path(Ks :: fjall_otx_ks:otx_ks()) -> binary().
otx_ks_path(_Ks) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

% fjall_iter NIFs
-spec ks_iter(Ks :: fjall_ks:ks(), Direction :: fjall:direction()) ->
    fjall:result(fjall_iter:iter()).
ks_iter(_Ks, _Direction) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_range(
    Ks :: fjall_ks:ks(),
    Direction :: fjall:direction(),
    Range :: fjall:range(),
    Start :: binary(),
    End :: binary()
) ->
    fjall:result(fjall_iter:iter()).
ks_range(_Ks, _Direction, _Range, _Start, _End) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec ks_prefix(Ks :: fjall_ks:ks(), Direction :: fjall:direction(), Prefix :: binary()) ->
    fjall:result(fjall_iter:iter()).
ks_prefix(_Ks, _Direction, _Prefix) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec otx_ks_iter(Ks :: fjall_otx_ks:otx_ks(), Direction :: fjall:direction()) ->
    fjall:result(fjall_iter:iter()).
otx_ks_iter(_Ks, _Direction) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_range(
    Ks :: fjall_otx_ks:otx_ks(),
    Direction :: fjall:direction(),
    Range :: fjall:range(),
    Start :: binary(),
    End :: binary()
) ->
    fjall:result(fjall_iter:iter()).
otx_ks_range(_Ks, _Direction, _Range, _Start, _End) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec otx_ks_prefix(
    Ks :: fjall_otx_ks:otx_ks(), Direction :: fjall:direction(), Prefix :: binary()
) ->
    fjall:result(fjall_iter:iter()).
otx_ks_prefix(_Ks, _Direction, _Prefix) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec iter_next(Iter :: fjall_iter:iter()) -> {ok, {binary(), binary()}} | done | {error, term()}.
iter_next(_Iter) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec iter_collect(Iter :: fjall_iter:iter()) -> {ok, [{binary(), binary()}]} | {error, term()}.
iter_collect(_Iter) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec iter_collect(Iter :: fjall_iter:iter(), Limit :: non_neg_integer()) ->
    {ok, [{binary(), binary()}]} | {error, term()}.
iter_collect(_Iter, _Limit) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec iter_collect_keys(Iter :: fjall_iter:iter()) -> {ok, [binary()]} | {error, term()}.
iter_collect_keys(_Iter) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec iter_collect_keys(Iter :: fjall_iter:iter(), Limit :: non_neg_integer()) ->
    {ok, [binary()]} | {error, term()}.
iter_collect_keys(_Iter, _Limit) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec iter_collect_values(Iter :: fjall_iter:iter()) -> {ok, [binary()]} | {error, term()}.
iter_collect_values(_Iter) -> erlang:nif_error({nif_not_loaded, ?MODULE}).
-spec iter_collect_values(Iter :: fjall_iter:iter(), Limit :: non_neg_integer()) ->
    {ok, [binary()]} | {error, term()}.
iter_collect_values(_Iter, _Limit) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec iter_destroy(Iter :: fjall_iter:iter()) -> ok.
iter_destroy(_Iter) -> erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF loader                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load() ->
    SoName =
        case code:priv_dir(fjall) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, "libfjall-native"]);
                    _ ->
                        filename:join([priv, "libfjall-native"])
                end;
            Dir ->
                filename:join(Dir, "libfjall-native")
        end,
    erlang:load_nif(SoName, 0).
