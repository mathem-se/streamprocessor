/*
 * Copyright (C) 2021 Robert Sahlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.streamprocessor.core.io;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.streamprocessor.core.coders.GenericRowCoder;

public class SchemaDestinations {

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    public static DynamicDestinations<Row, Row> schemaDestination() {
        return new SchemaDestination();
    }

    public static DynamicDestinations<Row, Row> schemaDestination(String projectId) {
        return new SchemaDestination(projectId);
    }

    public static class SchemaDestination extends DynamicDestinations<Row, Row> {

        static final long serialVersionUID = 897432972L;
        private String projectId;

        public SchemaDestination() {}

        public SchemaDestination(String projectId) {
            this.projectId = projectId;
        }

        @Override
        public Row getDestination(ValueInSingleWindow<Row> element) {
            return (Row) element.getValue();
        }

        @Override
        public TableDestination getTable(Row row) {
            Schema schema = row.getSchema();
            String entity = schema.getOptions().getValue("entity", String.class);
            String tableProjectId = projectId;

            TimePartitioning timePartitioning =
                    new TimePartitioning()
                            .setField(
                                    schema.getOptions()
                                            .getValueOrDefault("timePartitioningField", null))
                            .setExpirationMs(
                                    schema.getOptions()
                                            .getValueOrDefault(
                                                    "timePartitioningExpirationMs", null))
                            .setRequirePartitionFilter(
                                    schema.getOptions()
                                            .getValueOrDefault(
                                                    "timePartitioningRequirePartitionFilter",
                                                    false))
                            .setType(
                                    schema.getOptions()
                                            .getValueOrDefault("timePartitioningType", "DAY"));

            TableReference tableReference =
                    new TableReference()
                            .setProjectId(
                                    schema.getOptions()
                                            .getValueOrDefault("projectId", tableProjectId))
                            .setDatasetId(
                                    schema.getOptions()
                                            .getValue("datasetId", String.class)
                                            .replaceAll("-", "_"))
                            .setTableId(
                                    schema.getOptions()
                                            .getValueOrDefault("tableId", entity)
                                            .replaceAll("-", "_")
                                            .toLowerCase());

            return new TableDestination(
                    tableReference, "Table for entity " + entity, timePartitioning);
        }

        @Override
        public TableSchema getSchema(Row row) {
            return BigQueryUtils.toTableSchema(row.getSchema());
        }

        @Override
        public GenericRowCoder getDestinationCoder() {
            return new GenericRowCoder();
        }
    }
}
