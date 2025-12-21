-module(fjall).

-on_load(load/0).

-export([
    open/1,
    open/2,
    open_partition/2,
    insert/3,
    get/2,
    remove/2
]).

-export_type([
    keyspace/0,
    partition/0,
    config_option/0
]).

-opaque keyspace() :: reference().
-opaque partition() :: reference().

-type config_option() ::
    {manual_journal_persist, boolean()}
    | {flush_workers, pos_integer()}
    | {compaction_workers, pos_integer()}
    | {max_open_files, pos_integer()}
    | {cache_size, pos_integer()}
    | {max_journaling_size, pos_integer()}
    | {max_write_buffer_size, pos_integer()}
    | {fsync_ms, pos_integer() | undefined}
    | {temporary, boolean()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec open(Path :: file:name_all()) ->
    {ok, keyspace()} | {error, term()}.
open(Path) ->
    open(Path, []).

-spec open(Path :: file:name_all(), Options :: [config_option()]) ->
    {ok, keyspace()} | {error, term()}.
open(Path, Options) ->
    PathBinary = path_to_binary(Path),
    open_nif(PathBinary, Options).

-spec open_nif(Path :: binary(), Options :: [config_option()]) ->
    {ok, keyspace()} | {error, term()}.
open_nif(_Path, _Options) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec open_partition(Keyspace :: keyspace(), Name :: binary()) ->
    {ok, partition()} | {error, term()}.
open_partition(_Keyspace, _Name) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec insert(Partition :: partition(), Key :: binary(), Value :: binary()) ->
    ok | {error, term()}.
insert(_Partition, _Key, _Value) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec get(Partition :: partition(), Key :: binary()) ->
    {ok, binary()} | {error, term()}.
get(_Partition, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec remove(Partition :: partition(), Key :: binary()) ->
    ok | {error, term()}.
remove(_Partition, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private Helper Functions                                               %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec path_to_binary(Path :: file:name_all()) -> binary().
path_to_binary(Path) when is_binary(Path) ->
    Path;
path_to_binary(Path) when is_list(Path) ->
    case file:native_name_encoding() of
        utf8 ->
            unicode:characters_to_binary(Path, unicode, utf8);
        latin1 ->
            unicode:characters_to_binary(Path, latin1, utf8)
    end;
path_to_binary(Path) when is_atom(Path) ->
    atom_to_binary(Path, utf8).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NIF loader                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load() ->
    SoName =
        case code:priv_dir(?MODULE) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, libnative]);
                    _ ->
                        filename:join([priv, libnative])
                end;
            Dir ->
                filename:join(Dir, libnative)
        end,
    case erlang:load_nif(SoName, 0) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
