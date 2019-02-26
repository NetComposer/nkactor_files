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
-module(nkactor_files_s3_provider_actor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behavior(nkactor_actor).
-behavior(nkactor_files_provider).

-export([config/0, parse/2, request/4, sync_op/3, get/2]).
-export([files_upload/3, files_download/2, files_delete/2]).


-include("nkactor_files.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").
-include_lib("nkactor/include/nkactor.hrl").



%% ===================================================================
%% Actor callbacks
%% ===================================================================

%% @doc
config() ->
    #{
        resource => ?RES_FILES_S3_PROVIDERS,
        versions => [<<"0">>],
        verbs => [create, delete, deletecollection, get, list, patch, update, watch, upload],
        short_names => [],
        camel => <<"S3Provider">>,
        permanent => true,
        filter_fields => [

        ],
        sort_fields => [

        ],
        immutable_fields => [
            'spec.encryption_algo',
            'spec.hash_algo',
            'spec.bucket',
            'spec.path'
        ]
    }.



%% @doc
parse(Actor, _Req) ->
    Syntax = #{
        spec => #{
            region => binary,
            key => binary,
            secret => binary,
            bucket => binary,
            path => binary,
            scheme => {atom, [http, https]},
            host => binary,
            port => pos_integer,
            direct_download => boolean,
            direct_upload => boolean,
            direct_download_secs => pos_integer,
            direct_upload_secs => pos_integer,
            '__mandatory' => [key,secret,bucket]
        },
        '__mandatory' => [spec]
    },
    nkactor_files_provider:parse(Actor, Syntax).


%% @doc
request(Verb, Path, ActorId, Req) ->
    nkactor_files_provider:request(Verb, Path, ActorId, Req).


%% @doc
get(Actor, ActorSt) ->
    #{data:=Data} = Actor,
    Actor2 = case Data of
        #{spec:=#{secret:=_}=Spec} ->
            Actor#{data:=Data#{spec:=Spec#{secret => <<>>}}};
        _ ->
            Actor
    end,
    {ok, Actor2, ActorSt}.


%% @doc
sync_op({nkactor_files_get_upload_link, CT}, _From, #actor_st{actor=Actor}=ActorSt) ->
    #{data:=#{spec:=Spec}} = Actor,
    Reply = case
        maps:get(direct_upload, Spec, false) /= true orelse
        maps:is_key(encryption_algo, Spec) orelse
        maps:is_key(hash_algo, Spec)
    of
        true ->
                {error, storage_class_incompatible};
        false ->
            Name = nklib_util:luid(),
            CT = to_bin(CT),
            {Bucket, Path} = get_bucket(Name, Spec),
            TTL = maps:get(direct_upload_secs, Spec, ?FILE_DIRECT_UPLOAD_SECS),
            {Verb, Uri} = nkpacket_httpc_s3:make_put_url(Bucket, Path, CT, TTL, Spec),
            {ok, #{verb=>Verb, uri=>Uri, ttl=>TTL}, ActorSt}
    end,
    {reply, Reply, ActorSt};

sync_op({nkactor_files_get_direct_download_link, Id}, _From, #actor_st{actor=Actor}=ActorSt) ->
    #{data:=#{spec:=Spec}} = Actor,
    Reply = case
        maps:get(direct_upload, Spec, false) /= true orelse
        maps:is_key(encryption_algo, Spec) orelse
        maps:is_key(hash_algo, Spec)
    of
        true ->
            {error, storage_class_incompatible};
        false ->
            {Bucket, Path} = get_bucket(to_bin(Id), Spec),
            TTL = maps:get(direct_download_secs, Spec, ?FILE_DIRECT_DOWNLOAD_SECS),
            {Verb, Uri} = nkpacket_httpc_s3:make_get_url(Bucket, Path, TTL, Spec),
            {ok, #{verb=>Verb, uri=>Uri, ttl=>TTL}, ActorSt}
    end,
    {reply, Reply, ActorSt};

sync_op({nkactor_files_check_meta, Id}, _From, #actor_st{actor=Actor}=ActorSt) ->
    #{data:=#{spec:=Spec}} = Actor,
    {Bucket, Path} = get_bucket(to_bin(Id), Spec),
    {<<"HEAD">>, Url, Hds} = nkpacket_httpc_s3:get_meta(Bucket, Path, Spec),
    Reply = case request(<<"HEAD">>, Url, Hds, <<>>, Spec) of
        {ok, _, #{s3_headers:=Headers}} ->
            Headers2 = [{nklib_util:to_lower(Key), Val} || {Key, Val} <- Headers],
            ContentType = nklib_util:get_value(<<"content-type">>, Headers2, <<>>),
            Size = binary_to_integer(nklib_util:get_value(<<"content-length">>, Headers2, <<>>)),
            case Spec of
                #{max_size:=MaxSize} when Size > MaxSize ->
%%                    case nkfile:delete(SrvId, ?DOMAIN_PKG_ID_FILE, Spec, #{name=>Id}) of
%%                        ok ->
%%                            ?ACTOR_LOG(notice, "deleted file too large: ~p", [Id], ActorSt);
%%                        {error, Error} ->
%%                            ?ACTOR_LOG(warning, "could not delete file too large ~p: ~p", [Id, Error], ActorSt)
%%                    end,
                    {error, file_too_large};
                _ ->
                    {ok, #{content_type=>ContentType, size=>Size}}
            end;
        {error, Error} ->
            {error, Error}
    end,
    {reply, Reply, ActorSt};

sync_op(Op, From, ActorSt) ->
    nkactor_files_provider:sync_op(Op, From, ActorSt).


%% ===================================================================
%% Provider callbacks
%% ===================================================================


%% @doc
files_upload(#{name:=Name, data:=#{spec:=FileSpec}}, Body, Provider) ->
    #{data:=#{spec:=ProvSpec}} = Provider,
    {Bucket, Path} = get_bucket(Name, ProvSpec),
    CT = maps:get(content_type, FileSpec),
    Hash = crypto:hash(sha256, Body),
    #{data:=#{spec:=ProvSpec}} = Provider,
    {Method, Url, Hds} = nkpacket_httpc_s3:put_object(Bucket, Path, CT, Hash, ProvSpec),
    case request(Method, Url, Hds, Body, ProvSpec) of
        {ok, _Body, Meta} ->
            {ok, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
files_download(#{name:=Name}, Provider) ->
    #{data:=#{spec:=ProvSpec}} = Provider,
    {Bucket, Path} = get_bucket(Name, ProvSpec),
    {Method, Url, Hds} = nkpacket_httpc_s3:get_object(Bucket, Path, ProvSpec),
    request(Method, Url, Hds, <<>>, ProvSpec).


%% @doc
files_delete(#{name:=Name}, Provider) ->
    #{data:=#{spec:=ProvSpec}} = Provider,
    {Bucket, Path} = get_bucket(Name, ProvSpec),
    {Method, Url, Hds} = nkpacket_httpc_s3:delete(Bucket, Path, ProvSpec),
    case request(Method, Url, Hds, <<>>, ProvSpec) of
        {ok, _, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Internal
%% ===================================================================

%% @private
get_bucket(Name, #{bucket:=Bucket}=ProvSpec) ->
    Path1 = maps:get(path, ProvSpec, <<"/">>),
    Path2 = filename:join([<<"/">>, Path1, to_bin(Name)]),
    {Bucket, Path2}.


%% @private
request(Method, Url, Hds, Bin, Spec) ->
    PoolId = maps:get(hackney_pool, Spec, default),
    lager:error("NKLOG REQUEST ~p ~p ~p", [Method, Url, Hds]),

    case hackney:request(Method, Url, Hds, Bin, [{pool, PoolId}, with_body]) of
        {ok, 200, ReplyHds, Body} ->
            {ok, Body, #{s3_headers=>ReplyHds}};
        {ok, 204, ReplyHds, _} ->
            {ok,<<>>, #{s3_headers=>ReplyHds}};
        {ok, 403, _, B} ->
            lager:error("NKLOG B ~p", [B]),
            {error, unauthorized};
        {ok, 404, _, _} ->
            {error, file_not_found};
        {ok, Code, Hds, Body} ->
            {error, {http_error, Code, Hds, Body}};
        {ok, Code, Hds} ->
            {error, {http_error, Code, Hds, <<>>}};
        {error, Error} ->
            {error, Error}
    end.



%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).