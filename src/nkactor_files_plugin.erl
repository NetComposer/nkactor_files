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

%% @doc Default callbacks for plugin definitions
-module(nkactor_files_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3, plugin_start/3]).
-export_type([continue/0]).

-type continue() :: continue | {continue, list()}.

-include("nkactor_files.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc 
plugin_deps() ->
	[].


%% @doc
plugin_config(_SrvId, Config, #{class:=<<"Actor">>}) ->
	Syntax = #{
        file_providers => {list, #{
			name => binary,
			resource => binary,
			data => #{
				spec => map,
				'__mandatory' => spec,
				'__allow_unknown' => true
			},
			'__allow_unknown' => true,
			'__mandatory' => [name, resource, data]
		}}
    },
    nkserver_util:parse_config(Config, Syntax).


plugin_start(SrvId, Config, Service) ->
	% Give time to start base
	timer:sleep(500),
	Base = maps:get(base_namespace, Config),
	case maps:get(file_providers, Config, []) of
		[] ->
			ok;
		Providers ->
			case create_providers(Providers, Base, Service, []) of
				{ok, Providers2} ->
					nklib_util:do_config_put({nkactor_files_providers, SrvId}, Providers2);
				{error, Error} ->
					{error, Error}
			end
	end.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
create_providers([], _Base, _Service, Acc) ->
	{ok, Acc};

create_providers([#{name:=Name}=Actor|Rest], Base, Service, Acc) ->
	Actor2 = Actor#{
		group => ?GROUP_FILES,
		namespace => Base
	},
	case create_provider(Actor2) of
		{ok, Actor3} ->
			?SRV_LOG(notice, "provider '~s' created", [Name], Service),
			create_providers(Rest, Base, Service, [Actor3|Acc]);
		{error, Error} ->
			?SRV_LOG(error, "could not create provider '~s': ~p", [Name, Error], Service),
			{error, Error}
	end.


%% @private
create_provider(#{name:=Name}=Actor) ->
	UID = <<"provider-local-", Name/binary>>,
	case nkactor:create(Actor, #{forced_uid=>UID}) of
		{ok, Actor2} ->
			{ok, Actor2};
		{error, uniqueness_violation} ->
			ActorId = nkactor_lib:actor_to_actor_id(Actor),
			nkactor:stop(ActorId),
			timer:sleep(100),
			case nkactor:create(Actor, #{forced_uid=>UID}) of
				{ok, Actor2} ->
					{ok, Actor2};
				{error, Error} ->
					{error, Error}
			end;
		{error, Error} ->
			{error, Error}
	end.



