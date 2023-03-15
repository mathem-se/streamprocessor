package org.datahem.processor.pubsub.backfill;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 Robert Sahlin and MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

//import org.datahem.processor.utils.BackupToByteArrayFn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;

public class StreamBackfillPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(StreamBackfillPipeline.class);

    private static final String DYNAMODB = "dynamoDb";

    public interface StreamBackfillPipelineOptions extends DataflowPipelineOptions {

        @Description("Pub/Sub topic")
        String getPubsubTopic();

        void setPubsubTopic(String topic);

        @Description("BigQuery query")
        String getQuery();

        void setQuery(String query);

        @Description("Pipeline type")
        String getPipelineType();

        void setPipelineType(String pipelineType);
    }


    public static void main(String[] args) {
        StreamBackfillPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamBackfillPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("BigQuery SELECT job",
                        BigQueryIO
                                .readTableRows()
                                //.withTemplateCompatibility()
                                .fromQuery(options.getQuery())
                                .usingStandardSql())
                //.withoutValidation())
                .apply("TableRow to PubSubMessage",
                        ParDo.of(new DoFn<TableRow, PubsubMessage>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                TableRow row = c.element();
                                String b = (String) row.get("data");
                                byte[] payload = Base64.getDecoder().decode(b.getBytes());
                                //byte[] payload = b.getBytes();
                                List<TableRow> repeated = (List<TableRow>) row.get("attributes");
                                HashMap<String, String> attributes = repeated
                                        .stream()
                                        .collect(HashMap::new, (map, record) -> map.put((String) record.get("key"), (String) record.get("value")), HashMap::putAll);
                                PubsubMessage pubSubMessage = new PubsubMessage(payload, attributes);
                                c.output(pubSubMessage);
                            }
                        }));
        if (options.getPipelineType() == DYNAMODB) {
            pipeline
                .apply(
                    "Transform Dynamodb stream change events",
                    ParDo.of(new DynamodbFn()))
                    .apply(
                        "Serialize to Rows",
                        ParDo.of(
                                        new SerializeMessageToRowFn(
                                                SERIALIZED_SUCCESS_TAG,
                                                SERIALIZED_DEADLETTER_TAG,
                                                options.getProject(),
                                                options.getBigQueryDataset(),
                                                options.getSchemaCheckRatio()))
                                .withOutputTags(
                                        SERIALIZED_SUCCESS_TAG,
                                        TupleTagList.of(SERIALIZED_DEADLETTER_TAG)));
        }

        if (options.getDeadLetterTopic() != null) {
            serialized
                    .get(SERIALIZED_DEADLETTER_TAG)
                    .apply(
                            "Write to deadLetter pubsub topic",
                            PubsubIO.writeMessages().to(options.getDeadLetterTopic()));
        }

        PCollection<Row> tokenized =
                serialized
                        .get(SERIALIZED_SUCCESS_TAG)
                        .setCoder(coder)
                        .apply(
                                "De-identify Rows",
                                ParDo.of(new DeIdentifyFn(options.getFirestoreProjectId())))
                        .setCoder(coder);

                        if (options.getBigQueryDataset() != null) {
                            String projectId = options.getProject();
                            String bigQueryDataset = options.getBigQueryDataset();
                
                            WriteResult result =
                                    tokenized.apply(
                                            "Write de-identified Rows to BigQuery",
                                            BigQueryIO.<Row>write()
                                                    .withFormatFunction(r -> BigQueryUtils.toTableRow((Row) r))
                                                    .ignoreUnknownValues()
                                                    .withCreateDisposition(
                                                            BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                                    .withWriteDisposition(
                                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                                    .to(
                                                            SchemaDestinations.schemaDestination(
                                                                    projectId, bigQueryDataset))
                                                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                                    .withFailedInsertRetryPolicy(
                                                            InsertRetryPolicy.retryTransientErrors())
                                                    .withExtendedErrorInfo()
                                            // .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                                            // //https://issues.apache.org/jira/browse/BEAM-13954
                                            // .withAutoSharding()
                                            );
                
                            result.getFailedInsertsWithErr()
                                    .apply(
                                            MapElements.into(TypeDescriptors.strings())
                                                    .via(
                                                            x -> {
                                                                String message =
                                                                        (new StringBuilder())
                                                                                .append(
                                                                                        " The table was "
                                                                                                + x.getTable())
                                                                                .append(
                                                                                        " The row was "
                                                                                                + x.getRow())
                                                                                .append(
                                                                                        " The error was "
                                                                                                + x.getError())
                                                                                .toString();
                                                                LOG.error(
                                                                        "exception[FailedInsertsException] step[{}]"
                                                                                + " details[{}]",
                                                                        "Dynamodb.main()",
                                                                        message);
                                                                return "";
                                                            }));
                        }
                

        pipeline.run();
    }
}