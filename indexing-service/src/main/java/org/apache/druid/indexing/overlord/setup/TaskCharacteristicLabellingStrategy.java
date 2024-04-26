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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.query.DruidMetrics;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskCharacteristicLabellingStrategy implements TaskLabellingStrategy
{
  private final Map<String, String> labelByDataSource;
  private final Map<String, String> labelByTaskType;
  private final Map<String, Map<Object, String>> labelByTaskTags;

  @JsonCreator
  public TaskCharacteristicLabellingStrategy(
      @JsonProperty("labelByDataSource") @Nullable Map<String, String> labelByDataSource,
      @JsonProperty("labelByTaskType") @Nullable Map<String, String> labelByTaskType,
      @JsonProperty("labelByTaskTags") @Nullable Map<String, Map<Object, String>> labelByTaskTags
  )
  {
    this.labelByDataSource = labelByDataSource;
    this.labelByTaskType = labelByTaskType;
    this.labelByTaskTags = labelByTaskTags;
  }

  @Override
  public String determineTaskLabel(Task task)
  {
    String label;

    label = getTaskLabelFromTags(task);
    if (label != null) {
      return label;
    }

    label = fetchLabelFromMap(labelByDataSource, task.getDataSource());
    if (label != null) {
      return label;
    }

    label = fetchLabelFromMap(labelByTaskType, task.getType());
    if (label != null) {
      return label;
    }

    return null;
  }

  @JsonProperty
  public Map<String, String> getLabelByDataSource()
  {
    return labelByDataSource;
  }

  @JsonProperty
  public Map<String, String> getLabelByTaskType()
  {
    return labelByTaskType;
  }

  @JsonProperty
  public Map<String, Map<Object, String>> getLabelByTaskTags()
  {
    return labelByTaskTags;
  }

  private String getTaskLabelFromTags(Task task)
  {
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);

    if (tags == null || labelByTaskTags == null) {
      return null;
    }

    for (Map.Entry<String, Map<Object, String>> entry : labelByTaskTags.entrySet()) {
      String tag = entry.getKey();
      Map<Object, String> labelMap = entry.getValue();

      if (tags.containsKey(tag)) {
        String label = labelMap.get(tags.get(tag));
        if (label != null) {
          return label;
        }
      }
    }

    return null;
  }

  private String fetchLabelFromMap(Map<String, String> map, String key)
  {
    return (map != null) ? map.get(key) : null;
  }
}
