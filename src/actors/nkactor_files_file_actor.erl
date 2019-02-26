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

%% @doc NkActor File Actor
-module(nkactor_files_file_actor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behavior(nkactor_actor).

-export([op_get_body/1, op_get_download_link/1, op_get_media/1]).
-export([config/0, parse/2, request/4, sync_op/3]).


-include("nkactor_files.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").
-include_lib("nkactor/include/nkactor.hrl").



%% ===================================================================
%% Operations
%% ===================================================================

%% @doc
op_get_body(Id) ->
    nkactor_srv:sync_op(Id, nkactor_files_get_body, 60000).


%% @doc
op_get_download_link(Id) ->
    nkactor_srv:sync_op(Id, nkactor_files_get_download_link).


op_get_media(Id) ->
    case nkactor:activate(Id) of
        {ok, #actor_id{group=?GROUP_FILES, resource=?RES_FILES_FILES}=ActorId} ->
            nkactor_srv:sync_op(ActorId, nkactor_files_get_media);
        {ok, _} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end.




%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Behaviour callbacks
%% ===================================================================

%% @doc
config() ->
    #{
        resource => ?RES_FILES_FILES,
        versions => [<<"0">>],
        verbs => [create, delete, deletecollection, get, list, patch, update, watch, upload],
        filter_fields => [
            'spec.name',
            'spec.size',
            'spec.content_type',
            'spec.external_id'
        ],
        sort_fields => [
            'spec.name',
            'spec.size',
            'spec.content_type'
        ],
        field_type => #{
            'spec.size' => integer
        },
        short_names => [],
        immutable_fields => [
            'spec.content_type',
            'spec.provider',
            'spec.external_id',
            'spec.size',
            'spec.hash',
            'spec.password'
        ]
    }.


%% @doc
parse(Actor, #{verb:=create}=Req) ->
    Syntax = #{
        spec => #{
            content_type => binary,
            provider => binary,
            body_base64 => base64,
            body_binary => binary,
            external_id => binary,
            url => binary
        },
        '__mandatory' => [spec]
    },
    case nkactor_lib:parse_actor_data(Actor, Syntax) of
        {ok, Actor2} ->
            do_create(Actor2, Req);
        {error, Error} ->
            {error, Error}
    end;

%% We allow fields in case they didn't change
%% immutable_fields makes sure no one is changed
parse(Actor, #{verb:=update}) ->
    Syntax = #{
        spec => #{
            content_type => binary,
            provider => binary,
            external_id => binary,
            hash => binary,
            size => integer,
            password => binary
        },
        '__mandatory' => [spec]
    },
    nkactor_lib:parse_actor_data(Actor, Syntax).


%% @doc
request(get, [], ActorId, Req) ->
    Syntax = #{get_body_inline => boolean},
    case nkactor_lib:parse_request_params(Req, Syntax) of
        {ok, #{get_body_inline:=true}} ->
            case nkactor:get_actor(ActorId) of
                {ok, #{data:=Data}=Actor} ->
                    case op_get_body(ActorId) of
                        {ok, _CT, Body} ->
                            #{spec:=Spec} = Data,
                            Spec2 = Spec#{body_binary=>Body},
                            Actor2 = Actor#{data:=Data#{spec:=Spec2}},
                            {ok, Actor2};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        _ ->
            continue
    end;

request(upload, [], _ActorId, Req) ->
    % When coming from http, fill content_type with http's header
    Syntax = #{
        provider => binary,
        content_type => binary,
        '__mandatory' => [provider, content_type]
    },
    case nkactor_lib:parse_request_params(Req, Syntax) of
        {ok, #{provider:=Provider, content_type:=CT}} ->
            case Req of
                #{body := Body} ->
                    Body2 = #{
                        spec => #{
                            content_type => CT,
                            body_binary => Body,
                            provider => Provider
                        }
                    },
                    Req2 = Req#{
                        verb := create,
                        body := Body2
                    },
                    nkactor_request:request(Req2);
                _ ->
                    {error, request_body_invalid}
            end;
        {error, Error} ->
            {error, Error}
    end;

request(get, [<<"_download">>], ActorId, _Req) ->
    case op_get_body(ActorId) of
        {ok, CT, Body} ->
            {raw, {CT, Body}};
        {error, Error} ->
            {error, Error}
    end;

request(get, [<<"_rpc">>, <<"download_link">>], ActorId, _Req) ->
    case op_get_download_link(ActorId) of
        {ok, Url, 0} ->
            {ok, #{url=>Url}};
        {ok, Url, TTL} ->
            {ok, #{url=>Url, ttl_secs=>TTL}};
        {error, Error} ->
            {error, Error}
    end;

request(_Verb, _Path, _ActorId, _Req) ->
    continue.


%% @doc
sync_op(nkactor_files_get_body, _From, ActorSt) ->
    #actor_st{actor=#{data:=Data}=Actor} = ActorSt,
    LinkType = nkactor_lib:link_type(?GROUP_FILES, ?LINK_FILES_PROVIDERS),
    [ProviderUID] = nkactor_lib:get_linked_uids(LinkType, Actor),
    #{spec:=#{content_type:=CT}} = Data,
    Reply = case nkactor_files_provider:op_get_spec(ProviderUID) of
        {ok, _ProvActorId, Provider} ->
            case nkactor_files_provider:download(Actor, Provider) of
                {ok, Bin, _DownMeta} ->
                    {ok, CT, Bin};
                {error, file_not_found} ->
                    {error, file_read_error};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            ?ACTOR_LOG(warning, "FILEprovider error: ~p", [Error]),
            {error,provider_error}
    end,
    {reply, Reply, ActorSt};

sync_op(nkactor_files_get_download_link, _From, ActorSt) ->
    #actor_st{actor=#{data:=Data}=Actor} = ActorSt,
    % #{spec:=#{external_id:=Id}} = Data,
    LinkType = nkactor_lib:link_type(?GROUP_FILES, ?LINK_FILES_PROVIDERS),
    [ProvUID] = nkactor_lib:get_linked_uids(LinkType, Actor),
    Reply = case nkactor_files_provider:op_get_direct_download_link(ProvUID, <<>>) of
        {ok, Link, TTL} ->
            {ok, Link, TTL};
        {error, Error} ->
            {error, Error}
    end,
    {reply, Reply, ActorSt};

%%sync_op(nkactor_files_get_media, _From, #actor_st{actor=Actor}=ActorSt) ->
%%    #actor{id=#actor_id{uid=UID, name=Name}=ActorId, data=Data} = Actor,
%%    #{spec:=#{<<"contentType">>:=CT, size:=Size}} = Data,
%%    Media = #{
%%        <<"fileId">> => UID,
%%        content_type => CT,
%%        size => Size,
%%        <<"name">> => Name
%%    },
%%    {reply, {ok, ActorId, Media}, ActorSt};

sync_op(_Op, _From, _ActorSt) ->
    continue.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
do_create(Actor, #{params:=#{provider:=Provider}}=Req) ->
    #{data:=#{spec:=Spec}=Data} = Actor,
    do_create(Actor#{data:=Data#{spec:=Spec#{provider=>Provider}}}, Req);

do_create(#{data:=#{spec:=#{provider:=Provider}=Spec}=Data}=Actor, #{srv:=SrvId}) ->
    ProvId = nkactor_lib:id_to_actor_id(SrvId, Provider),
    case nkactor_files_provider:op_get_spec(ProvId) of
        {ok, ProvActorId, ProviderActor} ->
            LinkType = nkactor_lib:link_type(?GROUP_FILES, ?LINK_FILES_PROVIDERS),
            Actor2 = nkactor_lib:add_link(ProvActorId, Actor, LinkType),
            Spec2 = maps:without([body_binary, body_base64], Spec),
            Actor3 = Actor2#{data:=Data#{spec:=Spec2}},
            do_parse_upload(Spec, Actor3, ProviderActor);
        {error, _} ->
            {error, {provider_unknown, Provider}}
    end;

do_create(_Actor, _Req) ->
    {error, {field_missing, <<"spec.provider">>}}.


%% @private
do_parse_upload(#{body_base64:=Bin}=Spec, File, Provider) ->
    Spec2 = maps:remove(body_base64, Spec),
    do_parse_upload(Spec2#{body_binary=>Bin}, File, Provider);

do_parse_upload(#{url:=Url}=Spec, File, Provider) ->
    case nkactor_files_provider:get_url(Provider, Url) of
        {ok, CT, Body} ->
            Spec2 = maps:remove(url, Spec),
            Spec3 = Spec2#{body_binary=>Body, content_type=>CT},
            do_parse_upload(Spec3, File, Provider);
        {error, Error} ->
            {error, Error}
    end;

do_parse_upload(#{body_binary:=Bin, content_type:=_}, File, Provider) ->
    case nkactor_files_provider:upload(File, Bin, Provider) of
        {ok, File2, _UpMeta} ->
            {ok, File2};
        {error, Error} ->
            {error, Error}
    end;

do_parse_upload(#{body_binary:=_}, _File, _Provider) ->
    {error, {field_missing, <<"spec.content_type">>}};

do_parse_upload(#{external_id:=Id, content_type:=CT}=Spec, File, Provider) ->
    ProvActorId = nkactor_lib:get_actor_id(Provider),
    case nkactor_files_provider:op_get_check_meta(ProvActorId, Id) of
        {ok, #{content_type:=CT, size:=Size}} ->
            #{data:=Data} = File,
            Spec2 = Spec#{size => Size},
            {ok, File#{data:=Data#{spec:=Spec2}}};
        {ok, _} ->
            {error, content_type_invalid};
        {error, Error} ->
            {error, Error}
    end;

do_parse_upload(#{external_id:=_}, _File, _Provider) ->
    {error, {field_missing, <<"spec.content_type">>}};

do_parse_upload(_Spec, _File, _Provider) ->
    {error, {field_missing, <<"body_base64">>}}.




%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).