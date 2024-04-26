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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import java.util.Objects;

public class WorkerSelectWithTaskLabellingStrategy implements WorkerSelectStrategy
{
  private final WorkerSelectStrategy workerSelectStrategy;
  private final TaskLabellingStrategy taskLabellingStrategy;

  @JsonCreator
  public WorkerSelectWithTaskLabellingStrategy(
      @JsonProperty("workerSelectStrategy") WorkerSelectStrategy workerSelectStrategy,
      @JsonProperty("taskLabellingStrategy") TaskLabellingStrategy taskLabellingStrategy
  )
  {
    this.workerSelectStrategy = workerSelectStrategy;
    this.taskLabellingStrategy = taskLabellingStrategy;
  }

  @Override
  public ImmutableWorkerInfo findWorkerForTask(
      final WorkerTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableWorkerInfo> zkWorkers,
      final Task task
  )
  {
    return workerSelectStrategy.findWorkerForTask(config, zkWorkers, task);
  }

  @Override
  public String determineTaskLabel(Task task)
  {
    return task.getContextValue(Tasks.LABEL, taskLabellingStrategy.determineTaskLabel(task));
  }

  @JsonProperty
  public WorkerSelectStrategy getWorkerSelectStrategy()
  {
    return workerSelectStrategy;
  }

  @JsonProperty
  public TaskLabellingStrategy getTaskLabellingStrategy()
  {
    return taskLabellingStrategy;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerSelectWithTaskLabellingStrategy)) {
      return false;
    }
    WorkerSelectWithTaskLabellingStrategy that = (WorkerSelectWithTaskLabellingStrategy) o;
    return Objects.equals(workerSelectStrategy, that.workerSelectStrategy) && Objects.equals(
        taskLabellingStrategy,
        that.taskLabellingStrategy
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(workerSelectStrategy, taskLabellingStrategy);
  }

  @Override
  public String toString()
  {
    return "WorkerSelectWithTaskLabellingStrategy{" +
           "workerSelectStrategy=" + workerSelectStrategy +
           ", taskLabellingStrategy=" + taskLabellingStrategy +
           '}';
  }
}
