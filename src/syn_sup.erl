%% ==========================================================================================================
%% Syn - A global Process Registry and Process Group manager.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2015-2019 Roberto Ostinelli <roberto@ostinelli.net> and Neato Robotics, Inc.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.
%% ==========================================================================================================
-module(syn_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).
-export([get_process_name/2]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 10000, Type, [I]}).


%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> {ok, pid()} | {already_started, pid()} | shutdown.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Callbacks
%% ===================================================================
-spec init([]) ->
    {ok, {{supervisor:strategy(), non_neg_integer(), pos_integer()}, [supervisor:child_spec()]}}.
init([]) ->
    SynInstances = application:get_env(syn, syn_shards, 1),
    Children = [child_spec(syn_backbone, 0)] ++ lists:foldl(fun(I, Acc) ->  % TODO: do we need multiple syn_backbone processes here ?
        [
            child_spec(syn_backbone, I),
            child_spec(syn_groups, I),
            child_spec(syn_registry, I)
        ] ++ Acc
        end, [], lists:seq(1, SynInstances)
    ),
    {ok, {{one_for_one, 10, 10}, Children}}.

child_spec(Module, I) ->
    ProcessName = get_process_name(Module, I),
    #{
        id => ProcessName,
        start => {Module, start_link, [ProcessName, I]},
        type => worker,
        restart => permanent,
        modules => [Module]
    }.

get_process_name(Module, Id) when is_atom(Module) andalso is_integer(Id) ->
    IB = integer_to_binary(Id),
    ModuleBin = atom_to_binary(Module, latin1),
    binary_to_atom(<<ModuleBin/binary, "_", IB/binary>>, latin1).