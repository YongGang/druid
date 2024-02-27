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

package org.apache.druid.k8s.overlord.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskLaneConfig
{
  private final String label;
  private final double capacityRatio;
  private final String policy;

  @JsonCreator
  public TaskLaneConfig(
      @JsonProperty("label") String label,
      @JsonProperty("capacityRatio") double capacityRatio,
      @JsonProperty("policy") String policy
  )
  {
    this.label = label;
    this.capacityRatio = capacityRatio;
    this.policy = policy;
  }

  @JsonProperty
  public String getLabel()
  {
    return label;
  }

  @JsonProperty
  public double getCapacityRatio()
  {
    return capacityRatio;
  }

  @JsonProperty
  public String getPolicy()
  {
    return policy;
  }

  @Override
  public String toString()
  {
    return "TaskLaneConfig{" +
           "label=" + label +
           ", capacityRatio=" + capacityRatio +
           ", policy=" + policy +
           '}';
  }
}
