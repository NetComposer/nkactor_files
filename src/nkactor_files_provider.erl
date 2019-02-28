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
-module(nkactor_files_provider).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([op_get_spec/1, op_get_direct_download_link/2, op_get_upload_link/2,
         op_get_check_meta/2]).
-export([link_to_provider/2]).
-export([upload/3, download/2, delete/2]).
-export([get_url/2]).
-export([parse/2, request/4, sync_op/3]).


-include("nkactor_files.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").
-include_lib("nkactor/include/nkactor.hrl").

%%-define(UPLOAD_LINK_TTL_SECS, 5*60).


%% ===================================================================
%% Behaviour
%% ===================================================================

-callback files_upload(File::nkactor:actor(), Body::binary(), Provider::nkactor:actor()) ->
    {ok, Meta::map()} | {error, term()}.

-callback files_download(File::nkactor:actor(), Provider::nkactor:actor()) ->
    {ok, Body::binary(), Meta::map()} | {error, term()}.

-callback files_delete(File::nkactor:actor(), Provider::nkactor:actor()) ->
    {ok, Meta::map()} | {error, term()}.


%% ===================================================================
%% Operations
%% ===================================================================

%% @doc
-spec op_get_spec(nkservice_actor:id()) ->
    {ok, #actor_id{}, map()} | {error, term()}.

op_get_spec(Id) ->
    nkactor_srv:sync_op(Id, nkactor_files_get_spec).


%% @doc
op_get_direct_download_link(Id, ExternalId) ->
    nkactor_srv:sync_op(Id, {nkactor_files_get_direct_download_link, ExternalId}).


%% @doc
op_get_upload_link(Id, CT) ->
    nkactor_srv:sync_op(Id, {nkactor_files_get_upload_link, CT}).


%% @doc
op_get_check_meta(Id, ExternalId) ->
    nkactor_srv:sync_op(Id, {nkactor_files_check_meta, ExternalId}).



%% @private
link_to_provider(ProviderId, Actor) ->
    case op_get_spec(ProviderId) of
        {ok, ProvActorId, _} ->
            LinkType = nkactor_lib:link_type(?GROUP_FILES, ?LINK_FILES_PROVIDERS),
            {ok, nkactor_lib:add_link(ProvActorId, LinkType, Actor)};
        _ ->
            {error, {provider_invalid, ProviderId}}
    end.




%% @doc
-spec upload(File::nkactor:actor(), Body::binary(), Provider::nkactor:actor()) ->
    {ok, File::nkactor:actor(), Meta::map()} | {error, term()}.

upload(File, Body, #{group:=?GROUP_FILES, resource:=Res}=Provider) ->
    case nkactor_util:get_module(?GROUP_FILES, Res) of
        undefined ->
            error({provider_resource_unknown, Res});
        Mod ->
            case nkactor_files_lib:encode_body(File, Body, Provider) of
                {ok, File2, Body2, Meta1} ->
                    case Mod:files_upload(File2, Body2, Provider) of
                        {ok, Meta2} ->
                            {ok, File2, maps:merge(Meta1, Meta2)};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc
-spec download(File::nkactor:actor(), Provider::nkactor:actor()) ->
    {ok, Body::binary(), Meta::map()} | {error, term()}.

download(File, #{group:=?GROUP_FILES, resource:=Res}=Provider) ->
    case nkactor_util:get_module(?GROUP_FILES, Res) of
        undefined ->
            error({provider_resource_unknown, Res});
        Mod ->
            case Mod:files_download(File, Provider) of
                {ok, Body, Meta1} ->
                    case nkactor_files_lib:decode_body(File, Body, Provider) of
                        {ok, Body2, Meta2} ->
                            {ok, Body2, maps:merge(Meta1, Meta2)};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc
-spec delete(File::nkactor:actor(), Provider::nkactor:actor()) ->
    {ok, Body::binary(), Meta::map()} | {error, term()}.

delete(File, #{group:=?GROUP_FILES, resource:=Res}=Provider) ->
    case nkactor_util:get_module(?GROUP_FILES, Res) of
        undefined ->
            error({provider_resource_unknown, Res});
        Mod ->
            Mod:files_delete(File, Provider)
    end.



%% @doc
-spec get_url(Provider::nkactor:actor(), binary()) ->
    {ok, CT::binary(), Body::binary()} | {error, term()}.

get_url(#{uid:=UID, data:=#{spec:=Spec}}, Url) ->
    MaxSize = maps:get(max_size, Spec, 0),
    Opts = [
        follow_redirect,
        {pool, UID}
    ],
    case catch hackney:request(get, Url, [], <<>>, Opts) of
        {ok, 200, Hds, Ref} ->
            case hackney_headers:parse(<<"content-length">>, Hds) of
                Size when is_integer(Size) andalso (MaxSize==0 orelse Size =< MaxSize) ->
                    case hackney_headers:parse(<<"content-type">>, Hds) of
                        {T1, T2, _} ->
                            case hackney:body(Ref) of
                                {ok, Body} ->
                                    CT = <<T1/binary, $/, T2/binary>>,
                                    {ok, CT, Body};
                                {error, Error} ->
                                    {error, {hackney_error, Error}}
                            end;
                        _ ->
                            {error, file_read_error}
                    end;
                _ ->
                    {error, file_too_large}
            end;
        _ ->
            {error, file_read_error}
    end.



%% @doc
parse(Actor, #{spec:=Spec}=Syntax) ->
    Spec2 = Spec#{
        max_size => {integer, 0, none},
        encryption_algo => {atom, [aes_cfb128]},
        hash_algo => {atom, [sha256]}
    },
    nkactor_lib:parse_actor_data(Actor, Syntax#{spec:=Spec2}).



%% @doc
%% Redirect to files, adding parameter
request(Verb, [<<"files">>|Rest], ActorId, Req) when Verb==create; Verb==upload ->
    Params = maps:get(params, Req, #{}),
    case nkactor:get_actor(ActorId) of
        {ok, #{provider_id:=PROVIDER}=_Actor} ->
            Req2 =Req#{
                resource := ?RES_FILES_FILES,
                subresource := Rest,
                params := Params#{provider => PROVIDER}
            },
            Req3 = maps:remove(name, Req2),
            nkactor_request:request(Req3);
        {error, Error} ->
            {error, Error}
    end;

request(get, [<<"_rpc">>, <<"uploadLink">>], ActorId, Req) ->
    Syntax = #{content_type => binary, '__mandatory'=>content_type},
    case nkactor_lib:parse_request_params(Req, Syntax) of
        {ok, #{content_type:=CT}, _} ->
            case op_get_upload_link(ActorId, CT) of
                {ok, Method, Url, Name, TTL} ->
                    {ok, #{method=>Method, url=>Url, id=>Name, ttl_secs=>TTL}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end;

request(_Verb, _Path, _ActorId, _Req) ->
    continue.



%% @doc
sync_op(nkactor_files_get_spec, _From, #actor_st{actor_id=ActorId, actor=Actor}=ActorSt) ->
    {reply, {ok, ActorId, maps:remove(metadata, Actor)}, ActorSt};

sync_op({nkactor_files_get_direct_download_link, _Id}, _From, ActorSt) ->
    {reply, {error, storage_class_incompatible}, ActorSt};

sync_op({nkactor_files_check_meta, _Id}, _From, ActorSt) ->
    {reply, {error, storage_class_incompatible}, ActorSt};

sync_op(_Op, _From, _ActorSt) ->
    continue.

