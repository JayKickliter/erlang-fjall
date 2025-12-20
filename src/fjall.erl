-module(fjall).

-on_load(load/0).

-export([
    hello/0
]).

%% Public API

-spec hello() -> ok.
hello() ->
    hello_nif().

%% Private NIF stubs

-spec hello_nif() -> ok.
hello_nif() ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% NIF loader

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
