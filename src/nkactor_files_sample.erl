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
-module(nkactor_files_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile(export_all).
-compile(nowarn_export_all).

-include("nkactor_files.hrl").
-include_lib("nkserver/include/nkserver_module.hrl").


%% @doc Starts the service
start() ->
    Config = #{
        plugins => nkactor_files,
        file_providers => [
            #{
                resource => ?RES_FILES_FILESYSTEM_PROVIDERS,
                name => prov1,
                data => #{
                    spec => #{
                        path => "/tmp",
                        hash_algo => sha256
                    }
                }
            },
            #{
                resource => ?RES_FILES_FILESYSTEM_PROVIDERS,
                name => prov2,
                data => #{
                    spec => #{
                        path => "/tmp",
                        hash_algo => sha256,
                        encryption_algo => aes_cfb128
                    }
                }
            },
            #{
                resource => ?RES_FILES_S3_PROVIDERS,
                name => prov3,
                data => #{
                    spec => #{
                        encryption_algo => aes_cfb128,
                        scheme => http,
                        host => localhost,
                        port => 9000,
                        bucket => "bucket1",
                        key => "5UBED0Q9FB7MFZ5EWIOJ",
                        secret => "CaK4frX0uixBOh16puEsWEvdjQ3X3RTDvkvE+tUI"
                    }
                }
            }
        ]
    },
    nkserver:start_link(<<"Actor">>, ?MODULE, Config).




test_base() ->
    {ok, Prov1} = nkactor:get_actor("files:filesystem-providers:prov1.nkactor_files_sample"),
    {ok, Prov1} = nkactor:get_actor(<<"provider-local-prov1">>),
    {ok, Prov1} = nkactor:get_actor("filesystem-providers:prov1.nkactor_files_sample"),
    {ok, Prov1} = nkactor:get_actor("prov1.nkactor_files_sample"),

    #{
        group := ?GROUP_FILES,
        resource := ?RES_FILES_FILESYSTEM_PROVIDERS,
        name := <<"prov1">>,
        namespace := <<"nkactor_files_sample">>,
        uid := <<"provider-local-prov1">>,
        data := #{
            spec := #{
                path := <<"/tmp">>,
                hash_algo := sha256
            }
        }
    } = Prov1,

    ok = nkactor:stop("files:filesystem-providers:prov2.nkactor_files_sample"),
    timer:sleep(50),
    {error, not_activated} = nkactor:stop("files:filesystem-providers:prov2.nkactor_files_sample"),
    {ok, Prov2} = nkactor:get_actor("prov2.nkactor_files_sample"),

    #{
        group := ?GROUP_FILES,
        resource := ?RES_FILES_FILESYSTEM_PROVIDERS,
        name := <<"prov2">>,
        namespace := <<"nkactor_files_sample">>,
        uid := <<"provider-local-prov2">>,
        data := #{
            spec := #{
                path := <<"/tmp">>,
                hash_algo := sha256,
                encryption_algo := aes_cfb128
            }
        }
    } = Prov2,
    ok.



test_filesystem_1() ->

    File1 = #{
        group => ?GROUP_FILES,
        resource => ?RES_FILES_FILES,
        name => file1,
        namespace => ?MODULE,
        data => #{
            spec => #{
                provider => "prov1.nkactor_files_sample",
                content_type => any,
                body_binary => <<"123">>
            }
        }
    },
    {ok, File2} = nkactor:create(File1, #{}),
    #{
        group := ?GROUP_FILES,
        resource := ?RES_FILES_FILES,
        name := <<"file1">>,
        namespace := <<"nkactor_files_sample">>,
        data := #{
            spec := #{
                provider := <<"prov1.nkactor_files_sample">>,
                content_type := <<"any">>,
                hash := Hash1,
                size := 3
            } = Spec2
        } = Data2,
        uid := UID1,
        metadata := #{
            links := #{
                <<"provider-local-prov1">> := <<"io.netc.files.providers">>
            }
        }
    } = File2,
    false = maps:is_key(body_binary, Spec2),
    Hash1 = base64:encode(crypto:hash(sha256, <<"123">>)),
    {ok, <<"123">>} = file:read_file(<<"/tmp/", UID1/binary>>),

    Req1 = #{verb => get, uid => UID1},
    {ok, File2, _} = nkactor_request:request(Req1),

    Req2 = Req1#{verb => get, uid => UID1, params => #{get_body_inline => true}},
    {ok, File3, _} = nkactor_request:request(Req2),
    File3 = File2#{data:=Data2#{spec:=Spec2#{body_binary => <<"123">>}}},

    file:write_file(<<"/tmp/", UID1/binary>>, <<"124">>),
    {error, hash_invalid, _} = nkactor_request:request(Req2),
    file:write_file(<<"/tmp/", UID1/binary>>, <<"123">>),

    Req3 = Req1#{verb => get, uid => UID1, subresource => <<"_download">>},
    {raw, {<<"any">>, <<"123">>}, _} = nkactor_request:request(Req3),

    Req4 = Req1#{verb => get, uid => UID1, subresource => <<"_rpc/download_link">>},
    {error, storage_class_incompatible, _} = nkactor_request:request(Req4),

    nkactor:stop(UID1),
    ok.




test_filesystem_2() ->

    File1 = #{
        group => ?GROUP_FILES,
        resource => ?RES_FILES_FILES,
        name => file2,
        namespace => ?MODULE,
        data => #{
            spec => #{
                provider => "prov2.nkactor_files_sample",
                content_type => any,
                body_binary => <<"123">>
            }
        }
    },
    {ok, File2} = nkactor:create(File1, #{}),
    #{
        group := ?GROUP_FILES,
        resource := ?RES_FILES_FILES,
        name := <<"file2">>,
        namespace := <<"nkactor_files_sample">>,
        data := #{
            spec := #{
                provider := <<"prov2.nkactor_files_sample">>,
                content_type := <<"any">>,
                hash := Hash1,
                size := 3,
                password := _
            } = Spec2
        } = Data2,
        uid := UID2,
        metadata := #{
            links := #{
                <<"provider-local-prov2">> := <<"io.netc.files.providers">>
            }
        }
    } = File2,
    false = maps:is_key(body_binary, Spec2),
    Hash1 = base64:encode(crypto:hash(sha256, <<"123">>)),
    {ok, Read} = file:read_file(<<"/tmp/", UID2/binary>>),
    true = Read /= <<"123">>,

    Req1 = #{verb => get, uid => UID2},
    {ok, File2, _} = nkactor_request:request(Req1),

    Req2 = Req1#{verb => get, uid => UID2, params => #{get_body_inline => true}},
    {ok, File3, _} = nkactor_request:request(Req2),
    File3 = File2#{data:=Data2#{spec:=Spec2#{body_binary => <<"123">>}}},

    file:write_file(<<"/tmp/", UID2/binary>>, <<"123">>),
    {error, hash_invalid, _} = nkactor_request:request(Req2),
    file:write_file(<<"/tmp/", UID2/binary>>, Read),

    Req3 = Req1#{verb => get, uid => UID2, subresource => <<"_download">>},
    {raw, {<<"any">>, <<"123">>}, _} = nkactor_request:request(Req3),

    nkactor:stop(UID2),
    ok.




test_s3() ->
% export MINIO_ACCESS_KEY=5UBED0Q9FB7MFZ5EWIOJ; export MINIO_SECRET_KEY=CaK4frX0uixBOh16puEsWEvdjQ3X3RTDvkvE+tUI; minio server .

    File1 = #{
        group => ?GROUP_FILES,
        resource => ?RES_FILES_FILES,
        name => file3,
        namespace => ?MODULE,
        data => #{
            spec => #{
                provider => "prov3.nkactor_files_sample",
                content_type => any,
                body_binary => <<"123">>
            }
        }
    },
    {ok, File2} = nkactor:create(File1, #{}),
    #{
        group := ?GROUP_FILES,
        resource := ?RES_FILES_FILES,
        name := <<"file3">>,
        namespace := <<"nkactor_files_sample">>,
        data := #{
            spec := #{
                provider := <<"prov3.nkactor_files_sample">>,
                content_type := <<"any">>,
                size := 3,
                password := _
            } = Spec2
        } = Data2,
        uid := UID2,
        metadata := #{
            links := #{
                <<"provider-local-prov3">> := <<"io.netc.files.providers">>
            }
        }
    } = File2,
    false = maps:is_key(body_binary, Spec2),

    Req1 = #{verb => get, uid => UID2},
    {ok, File2, _} = nkactor_request:request(Req1),

    Req2 = Req1#{verb => get, uid => UID2, params => #{get_body_inline => true}},
    {ok, File3, _} = nkactor_request:request(Req2),
    File3 = File2#{data:=Data2#{spec:=Spec2#{body_binary => <<"123">>}}},

    Req3 = Req1#{verb => get, uid => UID2, subresource => <<"_download">>},
    {raw, {<<"any">>, <<"123">>}, _} = nkactor_request:request(Req3),

    Req4 = Req1#{verb => get, uid => UID2, subresource => <<"_rpc/">>},



    nkactor:stop(UID2),
    ok.



%
%%
%%% Test s3 with external config
%%test_s3() ->
%%    BaseProvider = #{
%%        id => test_s3_2a,
%%        storageClass => s3,
%%        encryptionAlgo => aes_cfb128,
%%        debug => true,
%%        s3Config => #{
%%            scheme => http,
%%            host => localhost,
%%            port => 9000,
%%            bucket => bucket1,
%%            key => "5UBED0Q9FB7MFZ5EWIOJ",
%%            secret => "CaK4frX0uixBOh16puEsWEvdjQ3X3RTDvkvE+tUI",
%%            hackney_pool => pool1
%%        }
%%    },
%%    {ok, Provider} = nkfile:parse_provider_spec(?SRV, file_pkg, BaseProvider),
%%    FileMeta = #{name=>n3, contentType=>any},
%%    {ok, #{password:=Pass}, _} = nkfile:upload(?SRV, file_pkg, Provider, FileMeta, <<"321">>),
%%    {ok, <<"321">>, #{s3_headers:=_}} = nkfile:download(?SRV, file_pkg, Provider, FileMeta#{password=>Pass}),
%%    ok.

%%
%%s1() -> <<"
%%    fileConfig = {
%%        storageClass = 'filesystem',
%%        filePath = '/tmp'
%%    }
%%
%%    file2 = startPackage('File', fileConfig)
%%
%%    function test_1()
%%        result, meta1 = file2.upload('123', {name='test1'})
%%        if result == 'ok' then
%%            body, meta2 = file2.download({name='test1'})
%%            return body, meta2
%%        else
%%            return 'error'
%%        end
%%    end
%%
%%">>.


actor_authorize(Req) ->
    {true, Req}.