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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkSchemaUtil {

  @Parameter private boolean isTableSchema;

  @Parameters(name = "isTableSchema={0}")
  private static Object[][] parameters() {
    return new Object[][] {{true}, {false}};
  }

  @TestTemplate
  public void testConvertFlinkSchemaToIcebergSchema() {
    ResolvedSchema flinkSchema =
        ResolvedSchema.of(
            Column.physical("id", DataTypes.INT().notNull()),
            Column.physical("name", DataTypes.STRING()) /* optional by default */,
            Column.physical("salary", DataTypes.DOUBLE().notNull()),
            Column.physical(
                "locations",
                DataTypes.MAP(
                    DataTypes.STRING(),
                    DataTypes.ROW(
                        DataTypes.FIELD("posX", DataTypes.DOUBLE().notNull(), "X field"),
                        DataTypes.FIELD("posY", DataTypes.DOUBLE().notNull(), "Y field")))),
            Column.physical("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable()),
            Column.physical("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable()),
            Column.physical("char", DataTypes.CHAR(10).notNull()),
            Column.physical("varchar", DataTypes.VARCHAR(10).notNull()),
            Column.physical("boolean", DataTypes.BOOLEAN().nullable()),
            Column.physical("tinyint", DataTypes.TINYINT()),
            Column.physical("smallint", DataTypes.SMALLINT()),
            Column.physical("bigint", DataTypes.BIGINT()),
            Column.physical("varbinary", DataTypes.VARBINARY(10)),
            Column.physical("binary", DataTypes.BINARY(10)),
            Column.physical("time", DataTypes.TIME()),
            Column.physical("timestampWithoutZone", DataTypes.TIMESTAMP()),
            Column.physical("timestampWithZone", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
            Column.physical("date", DataTypes.DATE()),
            Column.physical("decimal", DataTypes.DECIMAL(2, 2)),
            Column.physical("decimal2", DataTypes.DECIMAL(38, 2)),
            Column.physical("decimal3", DataTypes.DECIMAL(10, 1)),
            Column.physical("multiset", DataTypes.MULTISET(DataTypes.STRING().notNull())));

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get(), null),
            Types.NestedField.optional(1, "name", Types.StringType.get(), null),
            Types.NestedField.required(2, "salary", Types.DoubleType.get(), null),
            Types.NestedField.optional(
                3,
                "locations",
                Types.MapType.ofOptional(
                    24,
                    25,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(22, "posX", Types.DoubleType.get(), "X field"),
                        Types.NestedField.required(
                            23, "posY", Types.DoubleType.get(), "Y field")))),
            Types.NestedField.optional(
                4, "strArray", Types.ListType.ofOptional(26, Types.StringType.get())),
            Types.NestedField.optional(
                5, "intArray", Types.ListType.ofOptional(27, Types.IntegerType.get())),
            Types.NestedField.required(6, "char", Types.StringType.get()),
            Types.NestedField.required(7, "varchar", Types.StringType.get()),
            Types.NestedField.optional(8, "boolean", Types.BooleanType.get()),
            Types.NestedField.optional(9, "tinyint", Types.IntegerType.get()),
            Types.NestedField.optional(10, "smallint", Types.IntegerType.get()),
            Types.NestedField.optional(11, "bigint", Types.LongType.get()),
            Types.NestedField.optional(12, "varbinary", Types.BinaryType.get()),
            Types.NestedField.optional(13, "binary", Types.FixedType.ofLength(10)),
            Types.NestedField.optional(14, "time", Types.TimeType.get()),
            Types.NestedField.optional(
                15, "timestampWithoutZone", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(16, "timestampWithZone", Types.TimestampType.withZone()),
            Types.NestedField.optional(17, "date", Types.DateType.get()),
            Types.NestedField.optional(18, "decimal", Types.DecimalType.of(2, 2)),
            Types.NestedField.optional(19, "decimal2", Types.DecimalType.of(38, 2)),
            Types.NestedField.optional(20, "decimal3", Types.DecimalType.of(10, 1)),
            Types.NestedField.optional(
                21,
                "multiset",
                Types.MapType.ofRequired(28, 29, Types.StringType.get(), Types.IntegerType.get())));

    checkSchema(flinkSchema, icebergSchema);
  }

  @TestTemplate
  public void testMapField() {
    ResolvedSchema flinkSchema =
        ResolvedSchema.of(
            Column.physical(
                "map_int_long",
                DataTypes.MAP(DataTypes.INT(), DataTypes.BIGINT()).notNull()) /* Required */,
            Column.physical(
                "map_int_array_string",
                DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()), DataTypes.STRING())),
            Column.physical(
                "map_decimal_string", DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.STRING())),
            Column.physical(
                "map_fields_fields",
                DataTypes.MAP(
                        DataTypes.ROW(
                                DataTypes.FIELD("field_int", DataTypes.INT(), "doc - int"),
                                DataTypes.FIELD("field_string", DataTypes.STRING(), "doc - string"))
                            .notNull(), /* Required */
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                    "field_array",
                                    DataTypes.ARRAY(DataTypes.STRING()),
                                    "doc - array"))
                            .notNull() /* Required */)
                    .notNull() /* Required */));

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(
                0,
                "map_int_long",
                Types.MapType.ofOptional(4, 5, Types.IntegerType.get(), Types.LongType.get()),
                null),
            Types.NestedField.optional(
                1,
                "map_int_array_string",
                Types.MapType.ofOptional(
                    7,
                    8,
                    Types.ListType.ofOptional(6, Types.IntegerType.get()),
                    Types.StringType.get()),
                null),
            Types.NestedField.optional(
                2,
                "map_decimal_string",
                Types.MapType.ofOptional(
                    9, 10, Types.DecimalType.of(10, 2), Types.StringType.get())),
            Types.NestedField.required(
                3,
                "map_fields_fields",
                Types.MapType.ofRequired(
                    15,
                    16,
                    Types.StructType.of(
                        Types.NestedField.optional(
                            11, "field_int", Types.IntegerType.get(), "doc - int"),
                        Types.NestedField.optional(
                            12, "field_string", Types.StringType.get(), "doc - string")),
                    Types.StructType.of(
                        Types.NestedField.optional(
                            14,
                            "field_array",
                            Types.ListType.ofOptional(13, Types.StringType.get()),
                            "doc - array")))));

    checkSchema(flinkSchema, icebergSchema);
  }

  @TestTemplate
  public void testStructField() {
    ResolvedSchema flinkSchema =
        ResolvedSchema.of(
            Column.physical(
                "struct_int_string_decimal",
                DataTypes.ROW(
                        DataTypes.FIELD("field_int", DataTypes.INT()),
                        DataTypes.FIELD("field_string", DataTypes.STRING()),
                        DataTypes.FIELD("field_decimal", DataTypes.DECIMAL(19, 2)),
                        DataTypes.FIELD(
                            "field_struct",
                            DataTypes.ROW(
                                    DataTypes.FIELD("inner_struct_int", DataTypes.INT()),
                                    DataTypes.FIELD(
                                        "inner_struct_float_array",
                                        DataTypes.ARRAY(DataTypes.FLOAT())))
                                .notNull()) /* Row is required */)
                    .notNull()) /* Required */,
            Column.physical(
                "struct_map_int_int",
                DataTypes.ROW(
                        DataTypes.FIELD(
                            "field_map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())))
                    .nullable()) /* Optional */);

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(
                0,
                "struct_int_string_decimal",
                Types.StructType.of(
                    Types.NestedField.optional(5, "field_int", Types.IntegerType.get()),
                    Types.NestedField.optional(6, "field_string", Types.StringType.get()),
                    Types.NestedField.optional(7, "field_decimal", Types.DecimalType.of(19, 2)),
                    Types.NestedField.required(
                        8,
                        "field_struct",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                3, "inner_struct_int", Types.IntegerType.get()),
                            Types.NestedField.optional(
                                4,
                                "inner_struct_float_array",
                                Types.ListType.ofOptional(2, Types.FloatType.get())))))),
            Types.NestedField.optional(
                1,
                "struct_map_int_int",
                Types.StructType.of(
                    Types.NestedField.optional(
                        11,
                        "field_map",
                        Types.MapType.ofOptional(
                            9, 10, Types.IntegerType.get(), Types.IntegerType.get())))));

    checkSchema(flinkSchema, icebergSchema);
  }

  @TestTemplate
  public void testListField() {
    ResolvedSchema flinkSchema =
        ResolvedSchema.of(
            Column.physical(
                "list_struct_fields",
                DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("field_int", DataTypes.INT())))
                    .notNull()) /* Required */,
            Column.physical(
                "list_optional_struct_fields",
                DataTypes.ARRAY(
                        DataTypes.ROW(
                            DataTypes.FIELD(
                                "field_timestamp_with_local_time_zone",
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())))
                    .nullable()) /* Optional */,
            Column.physical(
                "list_map_fields",
                DataTypes.ARRAY(
                        DataTypes.MAP(
                                DataTypes.ARRAY(
                                    DataTypes.INT().notNull()), /* Key of map must be required */
                                DataTypes.ROW(
                                    DataTypes.FIELD("field_0", DataTypes.INT(), "doc - int")))
                            .notNull())
                    .notNull()) /* Required */);

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(
                0,
                "list_struct_fields",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        Types.NestedField.optional(3, "field_int", Types.IntegerType.get())))),
            Types.NestedField.optional(
                1,
                "list_optional_struct_fields",
                Types.ListType.ofOptional(
                    6,
                    Types.StructType.of(
                        Types.NestedField.optional(
                            5,
                            "field_timestamp_with_local_time_zone",
                            Types.TimestampType.withZone())))),
            Types.NestedField.required(
                2,
                "list_map_fields",
                Types.ListType.ofRequired(
                    11,
                    Types.MapType.ofOptional(
                        9,
                        10,
                        Types.ListType.ofRequired(7, Types.IntegerType.get()),
                        Types.StructType.of(
                            Types.NestedField.optional(
                                8, "field_0", Types.IntegerType.get(), "doc - int"))))));

    checkSchema(flinkSchema, icebergSchema);
  }

  private void checkSchema(ResolvedSchema flinkSchema, Schema icebergSchema) {
    if (isTableSchema) {
      assertThat(FlinkSchemaUtil.convert(TableSchema.fromResolvedSchema(flinkSchema)).asStruct())
          .isEqualTo(icebergSchema.asStruct());
      // The conversion is not a 1:1 mapping, so we just check iceberg types.
      assertThat(
              FlinkSchemaUtil.convert(
                      FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema)))
                  .asStruct())
          .isEqualTo(icebergSchema.asStruct());
    } else {
      assertThat(FlinkSchemaUtil.convert(flinkSchema).asStruct())
          .isEqualTo(icebergSchema.asStruct());
      // The conversion is not a 1:1 mapping, so we just check iceberg types.
      assertThat(
              FlinkSchemaUtil.convert(
                      FlinkSchemaUtil.toResolvedSchema(FlinkSchemaUtil.convert(icebergSchema)))
                  .asStruct())
          .isEqualTo(icebergSchema.asStruct());
    }
  }

  @Test
  public void testInconsistentTypes() {
    checkInconsistentType(
        Types.UUIDType.get(), new BinaryType(16), new BinaryType(16), Types.FixedType.ofLength(16));
    checkInconsistentType(
        Types.StringType.get(),
        new VarCharType(VarCharType.MAX_LENGTH),
        new CharType(100),
        Types.StringType.get());
    checkInconsistentType(
        Types.BinaryType.get(),
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(100),
        Types.BinaryType.get());
    checkInconsistentType(
        Types.TimeType.get(), new TimeType(), new TimeType(3), Types.TimeType.get());
    checkInconsistentType(
        Types.TimestampType.withoutZone(),
        new TimestampType(6),
        new TimestampType(3),
        Types.TimestampType.withoutZone());
    checkInconsistentType(
        Types.TimestampType.withZone(),
        new LocalZonedTimestampType(6),
        new LocalZonedTimestampType(3),
        Types.TimestampType.withZone());
  }

  private void checkInconsistentType(
      Type icebergType,
      LogicalType flinkExpectedType,
      LogicalType flinkType,
      Type icebergExpectedType) {
    assertThat(FlinkSchemaUtil.convert(icebergType)).isEqualTo(flinkExpectedType);
    assertThat(FlinkSchemaUtil.convert(FlinkSchemaUtil.toSchema(RowType.of(flinkType))).asStruct())
        .isEqualTo(Types.StructType.of(Types.NestedField.optional(0, "f0", icebergExpectedType)));
    assertThat(
            FlinkSchemaUtil.convert(FlinkSchemaUtil.toResolvedSchema(RowType.of(flinkType)))
                .asStruct())
        .isEqualTo(Types.StructType.of(Types.NestedField.optional(0, "f0", icebergExpectedType)));
  }

  @TestTemplate
  public void testConvertFlinkSchemaBaseOnIcebergSchema() {
    Schema baseSchema =
        new Schema(
            Lists.newArrayList(
                Types.NestedField.required(101, "int", Types.IntegerType.get()),
                Types.NestedField.optional(102, "string", Types.StringType.get())),
            Sets.newHashSet(101));

    Schema convertedSchema;
    if (isTableSchema) {
      TableSchema flinkSchema =
          TableSchema.builder()
              .field("int", DataTypes.INT().notNull())
              .field("string", DataTypes.STRING().nullable())
              .primaryKey("int")
              .build();
      convertedSchema = FlinkSchemaUtil.convert(baseSchema, flinkSchema);
    } else {
      ResolvedSchema flinkSchema =
          new ResolvedSchema(
              List.of(
                  Column.physical("int", DataTypes.INT().notNull()),
                  Column.physical("string", DataTypes.STRING().nullable())),
              Collections.emptyList(),
              UniqueConstraint.primaryKey("pk", List.of("int")));
      convertedSchema = FlinkSchemaUtil.convert(baseSchema, flinkSchema);
    }

    assertThat(convertedSchema.asStruct()).isEqualTo(baseSchema.asStruct());
    assertThat(convertedSchema.identifierFieldIds()).containsExactly(101);
  }

  @TestTemplate
  public void testConvertFlinkSchemaWithPrimaryKeys() {
    Schema icebergSchema =
        new Schema(
            Lists.newArrayList(
                Types.NestedField.required(1, "int", Types.IntegerType.get()),
                Types.NestedField.required(2, "string", Types.StringType.get())),
            Sets.newHashSet(1, 2));

    if (isTableSchema) {
      TableSchema tableSchema = FlinkSchemaUtil.toSchema(icebergSchema);
      assertThat(tableSchema.getPrimaryKey())
          .isPresent()
          .get()
          .satisfies(k -> assertThat(k.getColumns()).containsExactly("int", "string"));
    } else {
      ResolvedSchema resolvedSchema = FlinkSchemaUtil.toResolvedSchema(icebergSchema);
      assertThat(resolvedSchema.getPrimaryKey())
          .isPresent()
          .get()
          .satisfies(k -> assertThat(k.getColumns()).containsExactly("int", "string"));
    }
  }

  @TestTemplate
  public void testConvertFlinkSchemaWithNestedColumnInPrimaryKeys() {
    Schema icebergSchema =
        new Schema(
            Lists.newArrayList(
                Types.NestedField.required(
                    1,
                    "struct",
                    Types.StructType.of(
                        Types.NestedField.required(2, "inner", Types.IntegerType.get())))),
            Sets.newHashSet(2));

    if (isTableSchema) {
      assertThatThrownBy(() -> FlinkSchemaUtil.toSchema(icebergSchema))
          .isInstanceOf(ValidationException.class)
          .hasMessageStartingWith("Could not create a PRIMARY KEY")
          .hasMessageContaining("Column 'struct.inner' does not exist.");
    } else {
      assertThatThrownBy(() -> FlinkSchemaUtil.toResolvedSchema(icebergSchema))
          .isInstanceOf(ValidationException.class)
          .hasMessageStartingWith("Invalid primary key")
          .hasMessageContaining("Column 'struct.inner' does not exist.");
    }
  }
}
