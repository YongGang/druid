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

import com.google.inject.Provider;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.k8s.overlord.runnerstrategy.KubernetesRunnerStrategy;
import org.apache.druid.k8s.overlord.runnerstrategy.WorkerRunnerStrategy;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(EasyMockRunner.class)
public class KubernetesAndWorkerTaskRunnerFactoryTest extends EasyMockSupport
{

  @Mock KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory;
  @Mock Provider<HttpRemoteTaskRunnerFactory> httpRemoteTaskRunnerFactoryProvider;
  @Mock Provider<RemoteTaskRunnerFactory> remoteTaskRunnerFactoryProvider;

  @Test
  public void test_useHttpTaskRunner_asDefault()
  {
    KubernetesAndWorkerTaskRunnerFactory factory = new KubernetesAndWorkerTaskRunnerFactory(
        kubernetesTaskRunnerFactory,
        httpRemoteTaskRunnerFactoryProvider,
        remoteTaskRunnerFactoryProvider,
        new KubernetesAndWorkerTaskRunnerConfig(null, null),
        new WorkerRunnerStrategy()
    );

    HttpRemoteTaskRunnerFactory httpRemoteTaskRunnerFactory = EasyMock.createMock(HttpRemoteTaskRunnerFactory.class);
    EasyMock.expect(httpRemoteTaskRunnerFactoryProvider.get()).andReturn(httpRemoteTaskRunnerFactory);
    EasyMock.expect(httpRemoteTaskRunnerFactory.build()).andReturn(null);
    EasyMock.expect(kubernetesTaskRunnerFactory.build()).andReturn(null);

    replayAll();
    factory.build();
    verifyAll();
  }

  @Test
  public void test_specifyRemoteTaskRunner()
  {
    KubernetesAndWorkerTaskRunnerFactory factory = new KubernetesAndWorkerTaskRunnerFactory(
        kubernetesTaskRunnerFactory,
        httpRemoteTaskRunnerFactoryProvider,
        remoteTaskRunnerFactoryProvider,
        new KubernetesAndWorkerTaskRunnerConfig(null, "remote"),
        new WorkerRunnerStrategy()
    );

    RemoteTaskRunnerFactory remoteTaskRunnerFactory = EasyMock.createMock(RemoteTaskRunnerFactory.class);
    EasyMock.expect(remoteTaskRunnerFactoryProvider.get()).andReturn(remoteTaskRunnerFactory);
    EasyMock.expect(remoteTaskRunnerFactory.build()).andReturn(null);
    EasyMock.expect(kubernetesTaskRunnerFactory.build()).andReturn(null);

    replayAll();
    factory.build();
    verifyAll();
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_specifyIncorrectTaskRunner_shouldThrowException()
  {
    KubernetesAndWorkerTaskRunnerFactory factory = new KubernetesAndWorkerTaskRunnerFactory(
        kubernetesTaskRunnerFactory,
        httpRemoteTaskRunnerFactoryProvider,
        remoteTaskRunnerFactoryProvider,
        new KubernetesAndWorkerTaskRunnerConfig(null, "noop"),
        new KubernetesRunnerStrategy()
    );

    EasyMock.expect(remoteTaskRunnerFactoryProvider.get().build()).andReturn(null);
    EasyMock.expect(kubernetesTaskRunnerFactory.build()).andReturn(null);

    replayAll();
    factory.build();
    verifyAll();
  }
}
