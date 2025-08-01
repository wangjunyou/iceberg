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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class FlinkDynamicTableFactory
    implements DynamicTableSinkFactory, DynamicTableSourceFactory {
  static final String FACTORY_IDENTIFIER = "iceberg";
  private final FlinkCatalog catalog;

  public FlinkDynamicTableFactory() {
    this.catalog = null;
  }

  public FlinkDynamicTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
    ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
    Map<String, String> tableProps = resolvedCatalogTable.getOptions();
    ResolvedSchema resolvedSchema =
        ResolvedSchema.of(
            resolvedCatalogTable.getResolvedSchema().getColumns().stream()
                .filter(Column::isPhysical)
                .collect(Collectors.toList()));

    TableLoader tableLoader;
    if (catalog != null) {
      tableLoader = createTableLoader(catalog, objectIdentifier.toObjectPath());
    } else {
      tableLoader =
          createTableLoader(
              resolvedCatalogTable,
              tableProps,
              objectIdentifier.getDatabaseName(),
              objectIdentifier.getObjectName());
    }

    return new IcebergTableSource(
        tableLoader, resolvedSchema, tableProps, context.getConfiguration());
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
    ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
    Map<String, String> writeProps = resolvedCatalogTable.getOptions();
    ResolvedSchema resolvedSchema =
        ResolvedSchema.of(
            resolvedCatalogTable.getResolvedSchema().getColumns().stream()
                .filter(Column::isPhysical)
                .collect(Collectors.toList()));

    TableLoader tableLoader;
    if (catalog != null) {
      tableLoader = createTableLoader(catalog, objectIdentifier.toObjectPath());
    } else {
      tableLoader =
          createTableLoader(
              resolvedCatalogTable,
              writeProps,
              objectIdentifier.getDatabaseName(),
              objectIdentifier.getObjectName());
    }

    return new IcebergTableSink(
        tableLoader, resolvedSchema, context.getConfiguration(), writeProps);
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(FlinkCreateTableOptions.CATALOG_TYPE);
    options.add(FlinkCreateTableOptions.CATALOG_NAME);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(FlinkCreateTableOptions.CATALOG_DATABASE);
    options.add(FlinkCreateTableOptions.CATALOG_TABLE);
    return options;
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_IDENTIFIER;
  }

  private static TableLoader createTableLoader(
      ResolvedCatalogTable resolvedCatalogTable,
      Map<String, String> tableProps,
      String databaseName,
      String tableName) {
    Configuration flinkConf = new Configuration();

    Map<String, String> mergedProps = mergeSrcCatalogProps(tableProps);

    mergedProps.forEach(flinkConf::setString);

    String catalogName = flinkConf.get(FlinkCreateTableOptions.CATALOG_NAME);
    Preconditions.checkNotNull(
        catalogName,
        "Table property '%s' cannot be null",
        FlinkCreateTableOptions.CATALOG_NAME.key());

    String catalogDatabase = flinkConf.get(FlinkCreateTableOptions.CATALOG_DATABASE, databaseName);
    Preconditions.checkNotNull(catalogDatabase, "The iceberg database name cannot be null");

    String catalogTable = flinkConf.get(FlinkCreateTableOptions.CATALOG_TABLE, tableName);
    Preconditions.checkNotNull(catalogTable, "The iceberg table name cannot be null");

    org.apache.hadoop.conf.Configuration hadoopConf = FlinkCatalogFactory.clusterHadoopConf();
    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    FlinkCatalog flinkCatalog =
        (FlinkCatalog) factory.createCatalog(catalogName, mergedProps, hadoopConf);
    ObjectPath objectPath = new ObjectPath(catalogDatabase, catalogTable);

    // Create database if not exists in the external catalog.
    if (!flinkCatalog.databaseExists(catalogDatabase)) {
      try {
        flinkCatalog.createDatabase(
            catalogDatabase, new CatalogDatabaseImpl(Maps.newHashMap(), null), true);
      } catch (DatabaseAlreadyExistException e) {
        throw new AlreadyExistsException(
            e,
            "Database %s already exists in the iceberg catalog %s.",
            catalogName,
            catalogDatabase);
      }
    }

    // Create table if not exists in the external catalog.
    if (!flinkCatalog.tableExists(objectPath)) {
      try {
        flinkCatalog.createIcebergTable(objectPath, resolvedCatalogTable, true);
      } catch (TableAlreadyExistException e) {
        throw new AlreadyExistsException(
            e,
            "Table %s already exists in the database %s and catalog %s",
            catalogTable,
            catalogDatabase,
            catalogName);
      }
    }

    return TableLoader.fromCatalog(
        flinkCatalog.getCatalogLoader(), TableIdentifier.of(catalogDatabase, catalogTable));
  }

  /**
   * Merges source catalog properties with connector properties. Iceberg Catalog properties are
   * serialized as json in FlinkCatalog#getTable to be able to isolate catalog props from iceberg
   * table props, Here, we flatten and merge them back to use to create catalog.
   *
   * @param tableProps the existing table properties
   * @return a map of merged properties, with source catalog properties taking precedence when keys
   *     conflict
   */
  private static Map<String, String> mergeSrcCatalogProps(Map<String, String> tableProps) {
    String srcCatalogProps = tableProps.get(FlinkCreateTableOptions.SRC_CATALOG_PROPS_KEY);
    if (srcCatalogProps != null) {
      Map<String, String> mergedProps = Maps.newHashMap();
      FlinkCreateTableOptions createTableOptions =
          FlinkCreateTableOptions.fromJson(srcCatalogProps);

      mergedProps.put(FlinkCreateTableOptions.CATALOG_NAME.key(), createTableOptions.catalogName());
      mergedProps.put(
          FlinkCreateTableOptions.CATALOG_DATABASE.key(), createTableOptions.catalogDb());
      mergedProps.put(
          FlinkCreateTableOptions.CATALOG_TABLE.key(), createTableOptions.catalogTable());
      mergedProps.putAll(createTableOptions.catalogProps());

      tableProps.forEach(
          (k, v) -> {
            if (!FlinkCreateTableOptions.SRC_CATALOG_PROPS_KEY.equals(k)) {
              mergedProps.put(k, v);
            }
          });

      return Collections.unmodifiableMap(mergedProps);
    }

    return tableProps;
  }

  private static TableLoader createTableLoader(FlinkCatalog catalog, ObjectPath objectPath) {
    Preconditions.checkNotNull(catalog, "Flink catalog cannot be null");
    return TableLoader.fromCatalog(catalog.getCatalogLoader(), catalog.toIdentifier(objectPath));
  }
}
