#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script create split table t_user.
# tableCount:2
# shard key:id
# split table key:col

source ../common/env.sh

vtctldclient ApplyVSchema --vschema-file vschema_customer_sharded_split_table.json customer || fail "Failed to create split table vschema in sharded customer keyspace"
vtctldclient ApplySchema --sql-file create_customer_split_table.sql customer || fail "Failed to create split table in sharded customer keyspace"

