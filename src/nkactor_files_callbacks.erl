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

%% @doc Default plugin callbacks
-module(nkactor_files_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([msg/1]).
-export([actor_db_find/2, actor_db_read/2, actor_db_create/2]).

-include("nkactor_files.hrl").
-include_lib("nkactor/include/nkactor.hrl").


%% ===================================================================
%% Errors Callbacks
%% ===================================================================


msg(_)   		                    -> continue.




%% ===================================================================
%% Actor callbacks
%% ===================================================================

actor_db_find(SrvId, ActorId) ->
    case nklib_util:do_config_get({nkactor_files_providers, SrvId}) of
        List when is_list(List) ->
            %lager:error("NKLOG FIND ~p", [lager:pr(ActorId, ?MODULE)]),
            case find_provider(List, ActorId) of
                {ok, Actor} ->
                    {ok, nkactor_lib:actor_to_actor_id(Actor), #{}};
                continue ->
                    continue
            end;
        undefined ->
            continue92

    end.


actor_db_read(SrvId, ActorId) ->
    case nklib_util:do_config_get({nkactor_files_providers, SrvId}) of
        List when is_list(List) ->
            %lager:error("NKLOG READ ~p", [lager:pr(ActorId, ?MODULE)]),
            case find_provider(List, ActorId) of
                {ok, Actor} ->
                    {ok, Actor, #{}};
                continue ->
                    continue
            end;
        undefined ->
            continue
    end.


actor_db_create(_SrvId, _Actor) ->
    % lager:error("NKLOG CREATE ~p", [_Actor]),
    {error, persistence_not_defined}.


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
find_provider([], _ActorId) ->
    continue;

find_provider([Actor|Rest], ActorId) ->
    #{group:=Group, resource:=Res, name:=Name, namespace:=Namespace, uid:=UID} = Actor,
    case ActorId of
        #actor_id{uid=UID} ->
            {ok, Actor};
        #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} ->
            {ok, Actor};
        #actor_id{group=undefined, resource=Res, name=Name, namespace=Namespace} ->
            {ok, Actor};
        #actor_id{group=undefined, resource=undefined, name=Name, namespace=Namespace} ->
            {ok, Actor};
        _ ->
            find_provider(Rest, ActorId)
    end.
