%% @doc Fjall embedded key-value database for Erlang.
%%
%% This module provides a simple interface to the Fjall LSM-tree based
%% key-value database. All keys and values are binaries.
%%
%% == Example ==
%%
%% ```
%% {ok, Keyspace} = fjall:open("./mydb"),
%% {ok, Partition} = fjall:open_partition(Keyspace, <<"default">>),
%% ok = fjall:insert(Partition, <<"key">>, <<"value">>),
%% {ok, <<"value">>} = fjall:get(Partition, <<"key">>),
%% ok = fjall:remove(Partition, <<"key">>).
%% '''
%%
%% == Keyspaces and Partitions ==
%%
%% A keyspace is a database instance with its own data and configuration.
%% Within a keyspace, you can create multiple partitions, which are
%% logical separations of key-value data. Each partition maintains its
%% own index and operations are independent.
%%
%% == Configuration Options ==
%%
%% When opening a keyspace, you can pass configuration options to control
%% behavior like cache size, worker thread counts, and journaling settings.
%% See `config_option/0' for available options.

-module(fjall).

-on_load(load/0).

-export([
    open/1,
    open/2,
    open_partition/2,
    insert/3,
    get/2,
    remove/2,
    persist/2
]).

-export_type([
    keyspace/0,
    partition/0,
    config_option/0,
    persist_mode/0
]).

%% Opaque handle to a Fjall keyspace instance.
%%
%% Keyspaces are database instances that can contain multiple partitions.
%% Use `open/1' or `open/2' to create or open a keyspace.
-opaque keyspace() :: reference().

%% Opaque handle to a partition within a keyspace.
%%
%% Partitions are logical separations of data within a keyspace.
%% Use `open_partition/2' to open a partition for key-value operations.
-opaque partition() :: reference().

%% Configuration option for keyspace creation.
%%
%% Supported options:
%% <ul>
%%   <li>`{cache_size, pos_integer()}' - Total block cache size in bytes. Higher values
%%       improve read performance but consume more memory. Default: 256MB</li>
%%   <li>`{max_write_buffer_size, pos_integer()}' - Maximum in-memory write buffer (memtable)
%%       size in bytes. When exceeded, data is flushed to disk. Default: 64MB</li>
%%   <li>`{max_journaling_size, pos_integer()}' - Maximum write-ahead log (journal) size
%%       in bytes. Older journals are cleaned up as needed. Default: 512MB</li>
%%   <li>`{flush_workers, pos_integer()}' - Number of worker threads for flushing memtables
%%       to disk. Default: 2</li>
%%   <li>`{compaction_workers, pos_integer()}' - Number of worker threads for background
%%       compaction of SSTables. Default: 4</li>
%%   <li>`{max_open_files, pos_integer()}' - Maximum number of open file descriptors.
%%       Default: 1024</li>
%%   <li>`{manual_journal_persist, boolean()}' - If `true', journal persistence is manual
%%       and must be triggered explicitly. Default: `false'</li>
%%   <li>`{fsync_ms, pos_integer() | undefined}' - How often to fsync the journal.
%%       `undefined' disables periodic fsync. Default: undefined</li>
%%   <li>`{temporary, boolean()}' - If `true', the keyspace is temporary and will be
%%       deleted when closed. Default: `false'</li>
%% </ul>
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

%% Persist mode for keyspace persistence.
%%
%% Determines the durability guarantee when persisting a keyspace:
%% <ul>
%%   <li>`buffer' - Flush to OS buffers only. Data survives application crash
%%       but not power loss or OS crash.</li>
%%   <li>`sync_data' - Flush with fdatasync. Ensures data is written to disk,
%%       suitable for most file systems.</li>
%%   <li>`sync_all' - Flush with fsync. Strongest guarantee, ensuring both data
%%       and metadata are written to disk.</li>
%% </ul>
-type persist_mode() ::
    buffer
    | sync_data
    | sync_all.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API                                                             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Opens a keyspace at the given path with default configuration options.
%%
%% The path can be a string, binary, or atom representing a file system path.
%% If the keyspace already exists at the given path, it will be opened.
%% Otherwise, a new keyspace will be created.
%%
%% Returns `{ok, Keyspace}' on success or `{error, Reason}' on failure.
%%
%% == Errors ==
%%
%% Common errors include:
%% <ul>
%%   <li>Permission denied - insufficient permissions to access the path</li>
%%   <li>Invalid path - path contains invalid UTF-8 sequences</li>
%%   <li>Disk error - I/O error when accessing the file system</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% {ok, Keyspace} = fjall:open("/var/lib/mydb")
%% '''
-spec open(Path :: file:name_all()) ->
    {ok, keyspace()} | {error, term()}.
open(Path) ->
    open(Path, []).

%% @doc Opens a keyspace at the given path with custom configuration options.
%%
%% Configuration options allow fine-tuning of Fjall's behavior for specific
%% workloads. Options are provided as a list of tuples. Unknown options will
%% result in an error.
%%
%% Returns `{ok, Keyspace}' on success or `{error, Reason}' on failure.
%%
%% == Example ==
%%
%% ```
%% Options = [
%%     {cache_size, 512 * 1024 * 1024},  % 512MB cache
%%     {max_write_buffer_size, 128 * 1024 * 1024},  % 128MB memtable
%%     {flush_workers, 4}  % 4 flush threads
%% ],
%% {ok, Keyspace} = fjall:open("/var/lib/mydb", Options)
%% '''
%%
%% == See Also ==
%%
%% `config_option/0' for available configuration options.
-spec open(Path :: file:name_all(), Options :: [config_option()]) ->
    {ok, keyspace()} | {error, term()}.
open(Path, Options) ->
    PathBinary = path_to_binary(Path),
    open_nif(PathBinary, Options).

-spec open_nif(Path :: binary(), Options :: [config_option()]) ->
    {ok, keyspace()} | {error, term()}.
open_nif(_Path, _Options) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @doc Opens or creates a partition within a keyspace.
%%
%% Partitions provide logical separation of data within a single keyspace.
%% If a partition with the given name already exists, it is opened.
%% Otherwise, a new partition is created.
%%
%% The partition name must be a binary. Partition names are persistent and
%% will be recovered when the keyspace is reopened.
%%
%% Returns `{ok, Partition}' on success or `{error, Reason}' on failure.
%%
%% == Errors ==
%%
%% <ul>
%%   <li>`{error, disk_error}' - I/O error when accessing partition data</li>
%%   <li>`{error, corrupted}' - Partition data is corrupted</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% {ok, Keyspace} = fjall:open("./db"),
%% {ok, Partition} = fjall:open_partition(Keyspace, <<"users">>)
%% '''
-spec open_partition(Keyspace :: keyspace(), Name :: binary()) ->
    {ok, partition()} | {error, term()}.
open_partition(_Keyspace, _Name) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @doc Inserts or updates a key-value pair in the partition.
%%
%% If the key already exists, its value is overwritten.
%% Both keys and values must be binaries.
%%
%% Returns `ok' on success or `{error, Reason}' on failure.
%%
%% == Errors ==
%%
%% <ul>
%%   <li>`{error, disk_error}' - I/O error when writing to disk</li>
%%   <li>`{error, out_of_memory}' - Insufficient memory to buffer the write</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% ok = fjall:insert(Partition, <<"alice">>, <<"alice@example.com">>)
%% '''
-spec insert(Partition :: partition(), Key :: binary(), Value :: binary()) ->
    ok | {error, term()}.
insert(_Partition, _Key, _Value) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @doc Retrieves the value associated with a key from the partition.
%%
%% Returns `{ok, Value}' if the key exists, or `{error, not_found}' if the
%% key does not exist. Returns `{error, Reason}' on other errors.
%%
%% == Errors ==
%%
%% <ul>
%%   <li>`{error, not_found}' - Key does not exist in the partition</li>
%%   <li>`{error, disk_error}' - I/O error when reading from disk</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% case fjall:get(Partition, <<"alice">>) of
%%     {ok, Email} ->
%%         io:format("Alice's email: ~s~n", [Email]);
%%     {error, not_found} ->
%%         io:format("Alice not found~n");
%%     {error, Reason} ->
%%         io:format("Error: ~p~n", [Reason])
%% end
%% '''
-spec get(Partition :: partition(), Key :: binary()) ->
    {ok, binary()} | {error, term()}.
get(_Partition, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @doc Removes a key-value pair from the partition.
%%
%% If the key does not exist, this is a no-op and still returns `ok'.
%% Both keys and values must be binaries.
%%
%% Returns `ok' on success or `{error, Reason}' on failure.
%%
%% == Errors ==
%%
%% <ul>
%%   <li>`{error, disk_error}' - I/O error when writing to disk</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% ok = fjall:remove(Partition, <<"alice">>)
%% '''
-spec remove(Partition :: partition(), Key :: binary()) ->
    ok | {error, term()}.
remove(_Partition, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @doc Persists the keyspace to disk with the specified durability mode.
%%
%% This function flushes all in-memory data and journals to storage according
%% to the specified persist mode. It is useful when you want to ensure all
%% writes are durable, especially when using `{manual_journal_persist, true}'
%% configuration.
%%
%% The persist mode controls the durability guarantee:
%% <ul>
%%   <li>`buffer' - Fastest, least durable. Suitable for data that can be
%%       reconstructed after an application crash.</li>
%%   <li>`sync_data' - Good balance of performance and durability. Recommended
%%       for most applications.</li>
%%   <li>`sync_all' - Slowest, most durable. Ensures both data and metadata
%%       are written to disk. Recommended for critical data.</li>
%% </ul>
%%
%% Returns `ok' on success or `{error, Reason}' on failure.
%%
%% == Errors ==
%%
%% <ul>
%%   <li>`{error, disk_error}' - I/O error when writing to disk</li>
%% </ul>
%%
%% == Example ==
%%
%% ```
%% {ok, Keyspace} = fjall:open("./db"),
%% ok = fjall:insert(Partition, <<"key">>, <<"value">>),
%% ok = fjall:persist(Keyspace, sync_all)
%% '''
-spec persist(Keyspace :: keyspace(), Mode :: persist_mode()) ->
    ok | {error, term()}.
persist(_Keyspace, _Mode) ->
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
