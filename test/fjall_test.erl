-module(fjall_test).
-include_lib("eunit/include/eunit.hrl").

hello_test() ->
    ?assertEqual(ok, fjall:hello()),
    ok.
