%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc NkActor File Provider Actor
-module(nkactor_files_filesystem_provider_actor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behavior(nkactor_actor).
-behavior(nkactor_files_provider).


-export([config/0, parse/2, request/4, sync_op/3]).
-export([files_upload/3, files_download/2, files_delete/2]).

-include("nkactor_files.hrl").


%% ===================================================================
%% Behaviour callbacks
%% ===================================================================

%% @doc
config() ->
    #{
        resource => ?RES_FILES_FILESYSTEM_PROVIDERS,
        versions => [<<"0">>],
        verbs => [create, delete, deletecollection, get, list, patch, update, watch, upload],
        short_names => [],
        camel => <<"FilesystemProvider">>,
        permanent => true,
        filter_fields => [
        ],
        sort_fields => [
        ],
        immutable_fields => [
            'spec.encryption_algo',
            'spec.hash_algo',
            'spec.path'
        ]
    }.


%% @doc
parse(Actor, _ApiReq) ->
    Syntax = #{
        spec => #{
            path => binary,
            '__mandatory' => [path]
        },
        '__mandatory' => [spec]
    },
    nkactor_files_provider:parse(Actor, Syntax).


%% @doc
request(Verb, Path, ActorId, Req) ->
    nkactor_files_provider:request(Verb, Path, ActorId, Req).


%% @doc
sync_op(Op, From, ActorSt) ->
    nkactor_files_provider:sync_op(Op, From, ActorSt).


%% ===================================================================
%% Provider callbacks
%% ===================================================================


%% @doc
files_upload(File, Body, Provider) ->
    Path = get_path(File, Provider),
    case file:write_file(Path, Body) of
        ok ->
            {ok, #{file_path=>Path}};
        {error, Error} ->
            lager:warning("write error at ~s: ~p", [Path, Error]),
            {error, file_write_error}
    end.


%% @doc
files_download(File, Provider) ->
    Path = get_path(File, Provider),
    case file:read_file(Path) of
        {ok, Body} ->
            {ok, Body, #{file_path=>Path}};
        {error, Error} ->
            lager:warning("read error at ~s: ~p", [Path, Error]),
            {error, file_read_error}
    end.


%% @doc
files_delete(File, Provider) ->
    Path = get_path(File, Provider),
    case file:delete(Path) of
        ok ->
            {ok, #{}};
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Internal
%% ===================================================================

get_path(#{uid:=Name}, #{data:=#{spec:=#{path:=FilePath}}}) ->
    filename:join([<<"/">>, FilePath, Name]).

