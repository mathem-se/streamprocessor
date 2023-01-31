/* (C)2021 */
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

    public static DynamicDestinations<Row, Row> schemaDestination(
            String projectId, String datasetId) {
        return new SchemaDestination(projectId, datasetId);
    }

    public static class SchemaDestination extends DynamicDestinations<Row, Row> {

        static final long serialVersionUID = 897432972L;
        private String projectId;
        private String datasetId;

        public SchemaDestination() {}

        public SchemaDestination(String projectId, String datasetId) {
            this.projectId = projectId;
            this.datasetId = datasetId;
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
            String tableDatasetId = datasetId;

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
                                            .getValueOrDefault("datasetId", tableDatasetId)
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
            return new GenericRowCoder(projectId, datasetId);
        }
    }
}
