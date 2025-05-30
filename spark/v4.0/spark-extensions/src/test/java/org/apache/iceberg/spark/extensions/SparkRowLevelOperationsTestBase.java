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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.DataOperations.DELETE;
import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_DVS_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADD_POS_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.apache.iceberg.TableProperties.DATA_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.ORC_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class SparkRowLevelOperationsTestBase extends ExtensionsTestBase {

  private static final Random RANDOM = ThreadLocalRandom.current();

  @Parameter(index = 3)
  protected FileFormat fileFormat;

  @Parameter(index = 4)
  protected boolean vectorized;

  @Parameter(index = 5)
  protected String distributionMode;

  @Parameter(index = 6)
  protected boolean fanoutEnabled;

  @Parameter(index = 7)
  protected String branch;

  @Parameter(index = 8)
  protected PlanningMode planningMode;

  @Parameter(index = 9)
  protected int formatVersion;

  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5},"
              + " fanout = {6}, branch = {7}, planningMode = {8}, formatVersion = {9}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        FileFormat.ORC,
        true,
        WRITE_DISTRIBUTION_MODE_NONE,
        true,
        SnapshotRef.MAIN_BRANCH,
        LOCAL,
        2
      },
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        FileFormat.PARQUET,
        true,
        WRITE_DISTRIBUTION_MODE_NONE,
        false,
        "test",
        DISTRIBUTED,
        2
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        FileFormat.PARQUET,
        RANDOM.nextBoolean(),
        WRITE_DISTRIBUTION_MODE_HASH,
        true,
        null,
        LOCAL,
        2
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "clients", "1",
            "parquet-enabled", "false",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        FileFormat.AVRO,
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        false,
        "test",
        DISTRIBUTED,
        2
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        FileFormat.PARQUET,
        RANDOM.nextBoolean(),
        WRITE_DISTRIBUTION_MODE_HASH,
        true,
        null,
        LOCAL,
        3
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "clients",
            "1",
            "parquet-enabled",
            "false",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        FileFormat.AVRO,
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        false,
        "test",
        DISTRIBUTED,
        3
      },
    };
  }

  protected abstract Map<String, String> extraTableProperties();

  protected void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s', '%s' '%s', '%s' '%s', '%s' '%s', '%s' '%s', '%s' '%s')",
        tableName,
        DEFAULT_FILE_FORMAT,
        fileFormat,
        WRITE_DISTRIBUTION_MODE,
        distributionMode,
        SPARK_WRITE_PARTITIONED_FANOUT_ENABLED,
        String.valueOf(fanoutEnabled),
        DATA_PLANNING_MODE,
        planningMode.modeName(),
        DELETE_PLANNING_MODE,
        planningMode.modeName(),
        FORMAT_VERSION,
        formatVersion);

    switch (fileFormat) {
      case PARQUET:
        sql(
            "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
            tableName, PARQUET_VECTORIZATION_ENABLED, vectorized);
        break;
      case ORC:
        sql(
            "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
            tableName, ORC_VECTORIZATION_ENABLED, vectorized);
        break;
      case AVRO:
        assertThat(vectorized).isFalse();
        break;
    }

    Map<String, String> props = extraTableProperties();
    props.forEach(
        (prop, value) -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tableName, prop, value);
        });
  }

  protected void createAndInitTable(String schema) {
    createAndInitTable(schema, null);
  }

  protected void createAndInitTable(String schema, String jsonData) {
    createAndInitTable(schema, "", jsonData);
  }

  protected void createAndInitTable(String schema, String partitioning, String jsonData) {
    sql("CREATE TABLE %s (%s) USING iceberg %s", tableName, schema, partitioning);
    initTable();

    if (jsonData != null) {
      try {
        Dataset<Row> ds = toDS(schema, jsonData);
        ds.coalesce(1).writeTo(tableName).append();
        createBranchIfNeeded();
      } catch (NoSuchTableException e) {
        throw new RuntimeException("Failed to write data", e);
      }
    }
  }

  protected void append(String table, String jsonData) {
    append(table, null, jsonData);
  }

  protected void append(String table, String schema, String jsonData) {
    try {
      Dataset<Row> ds = toDS(schema, jsonData);
      ds.coalesce(1).writeTo(table).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to write data", e);
    }
  }

  protected void createOrReplaceView(String name, String jsonData) {
    createOrReplaceView(name, null, jsonData);
  }

  protected void createOrReplaceView(String name, String schema, String jsonData) {
    Dataset<Row> ds = toDS(schema, jsonData);
    ds.createOrReplaceTempView(name);
  }

  protected <T> void createOrReplaceView(String name, List<T> data, Encoder<T> encoder) {
    spark.createDataset(data, encoder).createOrReplaceTempView(name);
  }

  private Dataset<Row> toDS(String schema, String jsonData) {
    List<String> jsonRows =
        Arrays.stream(jsonData.split("\n"))
            .filter(str -> !str.trim().isEmpty())
            .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, Encoders.STRING());

    if (schema != null) {
      return spark.read().schema(schema).json(jsonDS);
    } else {
      return spark.read().json(jsonDS);
    }
  }

  protected void validateDelete(
      Snapshot snapshot, String changedPartitionCount, String deletedDataFiles) {
    validateSnapshot(snapshot, DELETE, changedPartitionCount, deletedDataFiles, null, null);
  }

  protected void validateCopyOnWrite(
      Snapshot snapshot,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDataFiles) {
    String operation = null == addedDataFiles && null != deletedDataFiles ? DELETE : OVERWRITE;
    validateSnapshot(
        snapshot, operation, changedPartitionCount, deletedDataFiles, null, addedDataFiles);
  }

  protected void validateMergeOnRead(
      Snapshot snapshot,
      String changedPartitionCount,
      String addedDeleteFiles,
      String addedDataFiles) {
    String operation = null == addedDataFiles && null != addedDeleteFiles ? DELETE : OVERWRITE;
    validateSnapshot(
        snapshot, operation, changedPartitionCount, null, addedDeleteFiles, addedDataFiles);
  }

  protected void validateSnapshot(
      Snapshot snapshot,
      String operation,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDeleteFiles,
      String addedDataFiles) {
    assertThat(snapshot.operation()).as("Operation must match").isEqualTo(operation);
    validateProperty(snapshot, CHANGED_PARTITION_COUNT_PROP, changedPartitionCount);
    validateProperty(snapshot, DELETED_FILES_PROP, deletedDataFiles);
    validateProperty(snapshot, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
    validateProperty(snapshot, ADDED_FILES_PROP, addedDataFiles);
    if (formatVersion >= 3) {
      validateProperty(snapshot, ADDED_DVS_PROP, addedDeleteFiles);
      assertThat(snapshot.summary()).doesNotContainKey(ADD_POS_DELETE_FILES_PROP);
    }
  }

  protected void validateProperty(Snapshot snapshot, String property, Set<String> expectedValues) {
    String actual = snapshot.summary().get(property);
    assertThat(actual)
        .as(
            "Snapshot property "
                + property
                + " has unexpected value, actual = "
                + actual
                + ", expected one of : "
                + String.join(",", expectedValues))
        .isIn(expectedValues);
  }

  protected void validateProperty(Snapshot snapshot, String property, String expectedValue) {
    if (null == expectedValue) {
      assertThat(snapshot.summary()).doesNotContainKey(property);
    } else {
      assertThat(snapshot.summary())
          .as("Snapshot property " + property + " has unexpected value.")
          .containsEntry(property, expectedValue);
    }
  }

  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected DataFile writeDataFile(Table table, List<GenericRecord> records) {
    try {
      OutputFile file =
          Files.localOutput(
              temp.resolve(fileFormat.addExtension(UUID.randomUUID().toString())).toFile());

      DataWriter<GenericRecord> dataWriter =
          Parquet.writeData(file)
              .forTable(table)
              .createWriterFunc(GenericParquetWriter::create)
              .overwrite()
              .build();

      try {
        for (GenericRecord record : records) {
          dataWriter.write(record);
        }
      } finally {
        dataWriter.close();
      }

      return dataWriter.toDataFile();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected String commitTarget() {
    return branch == null ? tableName : String.format("%s.branch_%s", tableName, branch);
  }

  @Override
  protected String selectTarget() {
    return branch == null ? tableName : String.format("%s VERSION AS OF '%s'", tableName, branch);
  }

  protected void createBranchIfNeeded() {
    if (branch != null && !branch.equals(SnapshotRef.MAIN_BRANCH)) {
      sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branch);
    }
  }

  // ORC currently does not support vectorized reads with deletes
  protected boolean supportsVectorization() {
    return vectorized && (isParquet() || isCopyOnWrite());
  }

  private boolean isParquet() {
    return fileFormat.equals(FileFormat.PARQUET);
  }

  private boolean isCopyOnWrite() {
    return extraTableProperties().containsValue(RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  protected void assertAllBatchScansVectorized(SparkPlan plan) {
    List<SparkPlan> batchScans = SparkPlanUtil.collectBatchScans(plan);
    assertThat(batchScans).hasSizeGreaterThan(0).allMatch(SparkPlan::supportsColumnar);
  }

  protected void createTableWithDeleteGranularity(
      String schema, String partitionedBy, DeleteGranularity deleteGranularity) {
    createAndInitTable(schema, partitionedBy, null /* empty */);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.DELETE_GRANULARITY, deleteGranularity);
  }
}
