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

package org.apache.druid.k8s.overlord;

import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;


public class KubernetesAndWorkerTaskRunnerFactory implements TaskRunnerFactory<KubernetesAndWorkerTaskRunner>
{
  public static final String TYPE_NAME = "k8sAndWorker";

  private final KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory;
  private final HttpRemoteTaskRunnerFactory httpRemoteTaskRunnerFactory;
  private final RemoteTaskRunnerFactory remoteTaskRunnerFactory;
  private final KubernetesAndWorkerTaskRunnerConfig kubernetesAndWorkerTaskRunnerConfig;

  private KubernetesAndWorkerTaskRunner runner;

  @Inject
  public KubernetesAndWorkerTaskRunnerFactory(
      KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory,
      HttpRemoteTaskRunnerFactory httpRemoteTaskRunnerFactory,
      RemoteTaskRunnerFactory remoteTaskRunnerFactory,
      KubernetesAndWorkerTaskRunnerConfig kubernetesAndWorkerTaskRunnerConfig
  )
  {
    this.kubernetesTaskRunnerFactory = kubernetesTaskRunnerFactory;
    this.httpRemoteTaskRunnerFactory = httpRemoteTaskRunnerFactory;
    this.remoteTaskRunnerFactory = remoteTaskRunnerFactory;
    this.kubernetesAndWorkerTaskRunnerConfig = kubernetesAndWorkerTaskRunnerConfig;
  }

  @Override
  public KubernetesAndWorkerTaskRunner build()
  {
    runner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunnerFactory.build(),
        getWorkerTaskRunner(kubernetesAndWorkerTaskRunnerConfig),
        kubernetesAndWorkerTaskRunnerConfig
    );
    return runner;
  }

  private WorkerTaskRunner getWorkerTaskRunner(KubernetesAndWorkerTaskRunnerConfig kubernetesAndWorkerTaskRunnerConfig)
  {
    RunnerStrategy runnerStrategy = kubernetesAndWorkerTaskRunnerConfig.getRunnerStrategy();
    String workerType = null;
    if (runnerStrategy instanceof WorkerRunnerStrategy) {
      workerType = ((WorkerRunnerStrategy) runnerStrategy).getWorkerType();
    } else if (runnerStrategy instanceof MixRunnerStrategy) {
      workerType = ((MixRunnerStrategy) runnerStrategy).getWorkerType();
    }

    return
        workerType == null || HttpRemoteTaskRunnerFactory.TYPE_NAME.equals(workerType) ?
        httpRemoteTaskRunnerFactory.build() : remoteTaskRunnerFactory.build();
  }

  @Override
  public KubernetesAndWorkerTaskRunner get()
  {
    return runner;
  }
}
