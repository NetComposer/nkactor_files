%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc NkFILE

-module(nkactor_files_lib).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([encode_body/3, decode_body/3]).

-include("nkactor_files.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkactor/include/nkactor.hrl").

-define(IV, <<"NetComposerFile.">>).


%% ===================================================================
%% Types
%% ===================================================================


%% @doc
encode_body(File, Body, Provider) ->
    #{data:=#{spec:=FileSpec}=FileData} = File,
    #{data:=#{spec:=ProvSpec}} = Provider,
    % Adds 'size' parameter
    case check_size(FileSpec, Body, ProvSpec) of
        {ok, FileSpec2} ->
            % Adds 'hash' parameter
            case set_hash(FileSpec2, Body, ProvSpec) of
                {ok, FileSpec3} ->
                    % Adds 'password' parameter (if not specified before)
                    case encrypt(FileSpec3, Body, ProvSpec) of
                        {ok, FileSpec4, Body2, Meta} ->
                            {ok, File#{data:=FileData#{spec:=FileSpec4}}, Body2, Meta};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
decode_body(File, Body, Provider) ->
    #{data:=#{spec:=FileSpec}} = File,
    #{data:=#{spec:=ProvSpec}} = Provider,
    case decrypt(FileSpec, Body, ProvSpec) of
        {ok, Body2, Meta} ->
            case check_hash(FileSpec, Body2, ProvSpec) of
                ok ->
                    {ok, Body2, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private Check size and adds 'size' param
check_size(FileSpec, Body, ProvSpec) ->
    MaxSize = maps:get(max_size, ProvSpec, 0),
    ByteSize = byte_size(Body),
    case MaxSize==0 orelse byte_size(Body) =< MaxSize of
        true ->
            {ok, FileSpec#{size=>ByteSize}};
        false ->
            {error, file_too_large}
    end.



%% @private Check hash and adds 'hash' param
set_hash(FileSpec, Body, ProvSpec) ->
    case maps:find(hash_algo, ProvSpec) of
        {ok, sha256} ->
            Hash = base64:encode(crypto:hash(sha256, Body)),
            {ok, FileSpec#{hash => Hash}};
        {ok, Other} ->
            {error, {hash_algo_unknown, Other}};
        error ->
            {ok, FileSpec}
    end.


%% @private
encrypt(FileSpec, Body, ProvSpec) ->
    case maps:find(encryption_algo, ProvSpec) of
        {ok, aes_cfb128} ->
            Start = nklib_date:epoch(usecs),
            Pass = case maps:find(password, FileSpec) of
                error ->
                    crypto:strong_rand_bytes(16);
                Pass0 ->
                    base64:decode(Pass0)
            end,
            case catch crypto:block_encrypt(aes_cfb128, Pass, ?IV, Body) of
                {'EXIT', _} ->
                    {error, encryption_error};
                Bin2 ->
                    FileSpec2 = FileSpec#{password => base64:encode(Pass)},
                    Meta = #{crypt_usecs => nklib_date:epoch(usecs) - Start},
                    {ok, FileSpec2, Bin2, Meta}
            end;
        {ok, Other} ->
            {error, {encryption_algo_unknown, Other}};
        error ->
            {ok, FileSpec, Body, #{}}
    end.


%% @private
decrypt(FileSpec, Bin, ProvSpec) ->
    case maps:find(encryption_algo, ProvSpec) of
        {ok, aes_cfb128} ->
            case FileSpec of
                #{password:=Pass} ->
                    Start = nklib_date:epoch(usecs),
                    Pass2 = base64:decode(Pass),
                    case catch crypto:block_decrypt(aes_cfb128, Pass2, ?IV, Bin) of
                        {'EXIT', _} ->
                            {error, decryption_error};
                        Bin2 ->
                            Meta2 = #{
                                crypt_usecs => nklib_date:epoch(usecs) - Start
                            },
                            {ok, Bin2, Meta2}
                    end;
                _ ->
                    {error, password_missing}
            end;
        {ok, Other} ->
            {error, {encryption_algo_unknown, Other}};
        error ->
            {ok, Bin, #{}}
    end.


%% @private
check_hash(FileSpec, Bin, ProvSpec) ->
    case maps:find(hash_algo, ProvSpec) of
        {ok, sha256} ->
            case FileSpec of
                #{hash:=Hash1} ->
                    Hash2 = base64:decode(Hash1),
                    case Hash2 == crypto:hash(sha256, Bin) of
                        true ->
                            ok;
                        false ->
                            {error, hash_invalid}
                    end;
                _ ->
                    {error, hash_is_missing}
            end;
        {ok, Other} ->
            {error, {hash_algo_unknown, Other}};
        error ->
            ok
    end.




%%%% @private
%%to_bin(T) when is_binary(T)-> T;
%%to_bin(T) -> nklib_util:to_binary(T).
