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

package org.apache.druid.client.indexing;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class ClientCompactionRunnerInfoTest
{
  @Test
  public void testMSQEngineWithHashedPartitionsSpecIsInvalid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new HashedPartitionsSpec(100, null, null),
        Collections.emptyMap(),
        null,
        null
    );
    CompactionConfigValidationResult validationResult = ClientCompactionRunnerInfo.validateCompactionConfig(
        compactionConfig,
        CompactionEngine.NATIVE
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: Invalid partitioning type[HashedPartitionsSpec]. Must be either 'dynamic' or 'range'",
        validationResult.getReason()
    );
  }

  @Test
  public void testMSQEngineWithMaxTotalRowsIsInvalid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(100, 100L),
        Collections.emptyMap(),
        null,
        null
    );
    CompactionConfigValidationResult validationResult = ClientCompactionRunnerInfo.validateCompactionConfig(
        compactionConfig,
        CompactionEngine.NATIVE
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: 'maxTotalRows' not supported with 'dynamic' partitioning",
        validationResult.getReason()
    );
  }

  @Test
  public void testMSQEngineWithDynamicPartitionsSpecIsValid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(100, null),
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertTrue(ClientCompactionRunnerInfo.validateCompactionConfig(compactionConfig, CompactionEngine.NATIVE)
                                         .isValid());
  }

  @Test
  public void testMSQEngineWithDimensionRangePartitionsSpecIsValid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DimensionRangePartitionsSpec(100, null, ImmutableList.of("partitionDim"), false),
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertTrue(ClientCompactionRunnerInfo.validateCompactionConfig(compactionConfig, CompactionEngine.NATIVE)
                                         .isValid());
  }

  @Test
  public void testMSQEngineWithQueryGranularityAllIsValid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(3, null),
        Collections.emptyMap(),
        new UserCompactionTaskGranularityConfig(Granularities.ALL, Granularities.ALL, false),
        null
    );
    Assert.assertTrue(ClientCompactionRunnerInfo.validateCompactionConfig(compactionConfig, CompactionEngine.NATIVE)
                                          .isValid());
  }

  @Test
  public void testMSQEngineWithRollupFalseWithMetricsSpecIsInValid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(3, null),
        Collections.emptyMap(),
        new UserCompactionTaskGranularityConfig(null, null, false),
        new AggregatorFactory[]{new LongSumAggregatorFactory("sum", "sum")}
    );
    CompactionConfigValidationResult validationResult = ClientCompactionRunnerInfo.validateCompactionConfig(
        compactionConfig,
        CompactionEngine.NATIVE
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: 'granularitySpec.rollup' must be true if 'metricsSpec' is specified",
        validationResult.getReason()
    );
  }

  @Test
  public void testMSQEngineWithUnsupportedMetricsSpecIsInValid()
  {
    // Aggregators having different input and ouput column names are unsupported.
    final String inputColName = "added";
    final String outputColName = "sum_added";
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(3, null),
        Collections.emptyMap(),
        new UserCompactionTaskGranularityConfig(null, null, null),
        new AggregatorFactory[]{new LongSumAggregatorFactory(outputColName, inputColName)}
    );
    CompactionConfigValidationResult validationResult = ClientCompactionRunnerInfo.validateCompactionConfig(
        compactionConfig,
        CompactionEngine.NATIVE
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: Different name[sum_added] and fieldName(s)[[added]] for aggregator",
        validationResult.getReason()
    );
  }

  @Test
  public void testMSQEngineWithRollupNullWithMetricsSpecIsValid()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(3, null),
        Collections.emptyMap(),
        new UserCompactionTaskGranularityConfig(null, null, null),
        new AggregatorFactory[]{new LongSumAggregatorFactory("sum", "sum")}
    );
    Assert.assertTrue(ClientCompactionRunnerInfo.validateCompactionConfig(compactionConfig, CompactionEngine.NATIVE)
                                         .isValid());
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      PartitionsSpec partitionsSpec,
      Map<String, Object> context,
      @Nullable UserCompactionTaskGranularityConfig granularitySpec,
      @Nullable AggregatorFactory[] metricsSpec
  )
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        10000,
        new Period(3600),
        createTuningConfig(partitionsSpec),
        granularitySpec,
        null,
        metricsSpec,
        null,
        null,
        CompactionEngine.MSQ,
        context
    );
    return config;
  }

  private static UserCompactionTaskQueryTuningConfig createTuningConfig(PartitionsSpec partitionsSpec)
  {
    final UserCompactionTaskQueryTuningConfig tuningConfig = new UserCompactionTaskQueryTuningConfig(
        40000,
        null,
        2000L,
        null,
        new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), null),
        partitionsSpec,
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                 .withLongEncoding(CompressionFactory.LongEncodingStrategy.AUTO)
                 .build(),
        2,
        1000L,
        TmpFileSegmentWriteOutMediumFactory.instance(),
        100,
        5,
        1000L,
        new Duration(3000L),
        7,
        1000,
        100,
        2
    );
    return tuningConfig;
  }
}
