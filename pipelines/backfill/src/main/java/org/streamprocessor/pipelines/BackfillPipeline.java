package org.streamprocessor.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.coders.GenericRowCoder;
import org.streamprocessor.core.io.SchemaDestinations;
import org.streamprocessor.core.transforms.DeIdentifyFn;
import org.streamprocessor.core.transforms.DynamodbFn;
import org.streamprocessor.core.transforms.SalesforceFn;
import org.streamprocessor.core.transforms.SerializeMessageToRowFn;

/**
 * Backfill pipeline.
 *
 * <p>Gets data from Bigquery backup table 1 1 Serialize to Beam Rows using Data Catalog schema 2)
 * De-identify fields using tokenization on tagged fields Tokens are stored in Firestore 3) Write 4)
 * Write to BigQuery (patch schema if needed) 5) Write Failures to Pubsub Dead Letter Queue
 *
 * @author Robert Sahlin
 * @version 1.0
 * @since 2022-09-28
 */
public class BackfillPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BackfillPipeline.class);

    private static final String DYNAMODB = "dynamodb";
    private static final String SALESFORCE = "salesforce";

    static final TupleTag<Row> SERIALIZED_SUCCESS_TAG =
            new TupleTag<Row>("Serialized success") {
                static final long serialVersionUID = 894723987432L;
            };

    static final TupleTag<PubsubMessage> SERIALIZED_DEADLETTER_TAG =
            new TupleTag<PubsubMessage>("Serialized deadletter") {
                static final long serialVersionUID = 89472335422L;
            };

    static final TupleTag<Row> DEIDENTIFY_SUCCESS_TAG =
            new TupleTag<Row>("deidentify success") {
                static final long serialVersionUID = 89472335672L;
            };

    static final TupleTag<Row> DEIDENTIFY_TOKENS_TAG =
            new TupleTag<Row>("deidentify tokens") {
                static final long serialVersionUID = 89472335673L;
            };

    static final TupleTag<Row> DEIDENTIFY_FAILURE_TAG =
            new TupleTag<Row>("deidentify failure") {
                static final long serialVersionUID = 89472335674L;
            };

    static final TupleTag<PubsubMessage> JSON_MESSAGE_TAG =
            new TupleTag<PubsubMessage>("jsonMessage") {
                static final long serialVersionUID = 89472334422L;
            };

    public interface BackfillPipelineOptions extends DataflowPipelineOptions {

        @Description("BigQuery query")
        String getBackfillQuery();

        void setBackfillQuery(String backfillQuery);

        @Description("Pipeline type")
        String getPipelineType();

        void setPipelineType(String pipelineType);

        @Description("Firestore Project Id, if other than the project where Dataflow job runs")
        String getFirestoreProjectId();

        void setFirestoreProjectId(String value);

        @Description("Pubsub topic for deadletter output")
        String getDeadLetterTopic();

        void setDeadLetterTopic(String value);

        @Description("BigQuery Dataset")
        String getBigQueryDataset();

        void setBigQueryDataset(String value);

        @Description("Schema check sample ratio")
        @Default.Float(0.01f)
        float getSchemaCheckRatio();

        void setSchemaCheckRatio(float value);
    }

    public static void main(String[] args) {
        BackfillPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(BackfillPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        GenericRowCoder coder =
                new GenericRowCoder(options.getProject(), options.getBigQueryDataset());
        coderRegistry.registerCoderForClass(Row.class, coder);

        PCollection<PubsubMessage> pubsubMessages =
                pipeline.apply(
                                "BigQuery SELECT job",
                                BigQueryIO.readTableRows()
                                        // .withTemplateCompatibility()
                                        .fromQuery(options.getBackfillQuery())
                                        .usingStandardSql())
                        // .withoutValidation())
                        .apply(
                                "TableRow to PubSubMessage",
                                ParDo.of(
                                        new DoFn<TableRow, PubsubMessage>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                TableRow row = c.element();
                                                String b = (String) row.get("data");
                                                byte[] payload =
                                                        Base64.getDecoder().decode(b.getBytes());
                                                // byte[] payload = b.getBytes();
                                                List<TableRow> repeated =
                                                        (List<TableRow>) row.get("attributes");
                                                HashMap<String, String> attributes =
                                                        repeated.stream()
                                                                .collect(
                                                                        HashMap::new,
                                                                        (map, record) ->
                                                                                map.put(
                                                                                        (String)
                                                                                                record
                                                                                                        .get(
                                                                                                                "key"),
                                                                                        (String)
                                                                                                record
                                                                                                        .get(
                                                                                                                "value")),
                                                                        HashMap::putAll);
                                                PubsubMessage pubSubMessage =
                                                        new PubsubMessage(payload, attributes);
                                                c.output(pubSubMessage);
                                            }
                                        }));

        PCollection<PubsubMessage> transformed;
        if (options.getPipelineType().equals(DYNAMODB)) {
            transformed =
                    pubsubMessages.apply(
                            "Transform Dynamodb stream change events", ParDo.of(new DynamodbFn()));
        } else if (options.getPipelineType().equals(SALESFORCE)) {
            transformed =
                    pubsubMessages.apply(
                            "Transform salesforce events",
                            ParDo.of(new SalesforceFn()));

        } else {
            throw new RuntimeException("Pipeline type not supported");
        }

        PCollectionTuple serialized =
                transformed.apply(
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
                                                        "exception[FailedInsertsException]"
                                                                + " step[{}] details[{}]",
                                                        "Dynamodb.main()",
                                                        message);
                                                return "";
                                            }));
        }

        pipeline.run();
    }
}
