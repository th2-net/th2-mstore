/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.messagestore;

import com.exactpro.th2.store.common.CassandraConfig;

import java.util.Set;

public class StoreConfig {
  private CassandraConfig cassandraConfig;
  private Set<String> connectivityEndpoints;

  public StoreConfig() {
  }

  public StoreConfig(CassandraConfig cassandraConfig, Set<String> connectivityEndpoints) {
    this.cassandraConfig = cassandraConfig;
    this.connectivityEndpoints = connectivityEndpoints;
  }

  public CassandraConfig getCassandraConfig() {
    return cassandraConfig;
  }

  public void setCassandraConfig(CassandraConfig cassandraConfig) {
    this.cassandraConfig = cassandraConfig;
  }

  public Set<String> getConnectivityEndpoints() {
    return connectivityEndpoints;
  }

  public void setConnectivityEndpoints(Set<String> connectivityEndpoints) {
    this.connectivityEndpoints = connectivityEndpoints;
  }
}
