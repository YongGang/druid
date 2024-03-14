/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.indexing.common.task.DefaultTaskIdentitiesProvider;
import org.apache.druid.indexing.common.task.TaskIdentitiesProvider;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.server.initialization.IndexerZkConfig;

/**
 */
public class IndexingServiceModuleHelper
{
  public static final String INDEXER_RUNNER_PROPERTY_PREFIX = "druid.indexer.runner";

  public static void configureTaskRunnerConfigs(Binder binder)
  {
    JsonConfigProvider.bind(binder, INDEXER_RUNNER_PROPERTY_PREFIX, ForkingTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, INDEXER_RUNNER_PROPERTY_PREFIX, RemoteTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, INDEXER_RUNNER_PROPERTY_PREFIX, HttpRemoteTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.zk.paths.indexer", IndexerZkConfig.class);

    configureTaskLabelsProvider(binder);
  }

  public static void configureTaskLabelsProvider(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(TaskIdentitiesProvider.class))
            .addBinding(DefaultTaskIdentitiesProvider.TYPE)
            .to(DefaultTaskIdentitiesProvider.class)
            .in(LazySingleton.class);

    PolyBind.createChoiceWithDefault(
        binder,
        "druid.indexer.task.labelsprovider.type",
        Key.get(TaskIdentitiesProvider.class),
        DefaultTaskIdentitiesProvider.TYPE
    );
  }
}
