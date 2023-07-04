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

package org.streamprocessor.pipelines;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.application.StreamProcessorOptions;
import org.streamprocessor.core.coders.FailsafeElementCoder;
import org.streamprocessor.core.coders.GenericRowCoder;
import org.streamprocessor.core.io.SchemaDestinations;
import org.streamprocessor.core.transforms.DeIdentifyFn;
import org.streamprocessor.core.transforms.ExtractCurrentElementFn;
import org.streamprocessor.core.transforms.FailSafeElementToPubsubMessageFn;
import org.streamprocessor.core.transforms.RowToPubsubMessageFn;
import org.streamprocessor.core.transforms.SerializeMessageToRowFn;
import org.streamprocessor.core.transforms.TransformMessageFn;
import org.streamprocessor.core.values.FailsafeElement;

/**
 * Streamer pipeline
 *
 * <p>Receives pub/sub messages with Json payload and serialize them to beam rows using data catalog
 * schemas and then tokenize fields based on tags and then writes to BigQuery and pub/sub topic.
 * Stages: 1) Read JSON from Pubsub subscription 2) Serialize to Beam Rows using Data Catalog schema
 * 3) De-identify fields using tokenization on tagged fields Tokens are stored in Firestore 4) Write
 * to BigQuery (patch schema if needed) 5) Write to Pubsub topics for streaming analytics 6) Write
 * to Pubsub backup topic 7) Write Failures to Pubsub Dead Letter Queue
 *
 * @author Robert Sahlin
 * @version 1.0
 * @since 2022-09-28
 */
public class DataContracts {

    private static final Logger LOG = LoggerFactory.getLogger(DataContracts.class);

    static final TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>>
            TRANSFORM_MESSAGE_SUCCESS_TAG =
                    new TupleTag<>("Transform message success") {
                        static final long serialVersionUID = 7062806547763956169L;
                    };
    static final TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>>
            TRANSFORM_MESSAGE_FAILURE_TAG =
                    new TupleTag<>("Transform message failure") {
                        static final long serialVersionUID = 7391614518888199305L;
                    };
    static final TupleTag<FailsafeElement<PubsubMessage, Row>> SERIALIZED_SUCCESS_TAG =
            new TupleTag<>("Serialized success") {
                static final long serialVersionUID = 894723987432L;
            };
    static final TupleTag<FailsafeElement<PubsubMessage, Row>> SERIALIZED_FAILURE_TAG =
            new TupleTag<>("Serialized failure") {
                static final long serialVersionUID = 89472335422L;
            };

    static final TupleTag<FailsafeElement<PubsubMessage, Row>> DEIDENTIFY_SUCCESS_TAG =
            new TupleTag<>("De-identify success") {
                static final long serialVersionUID = 89472335672L;
            };
    static final TupleTag<FailsafeElement<PubsubMessage, Row>> DEIDENTIFY_FAILURE_TAG =
            new TupleTag<>("De-identify failure") {
                static final long serialVersionUID = 89472335674L;
            };

    /**
     * Main entry point. Runs a pipeline which reads data from pubsub and writes to BigQuery and
     * pubsub.
     *
     * @param args the command line arguments to the pipeline
     */
    public static void main(String[] args) {
        StreamProcessorOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(StreamProcessorOptions.class);

        List<String> serviceOptions = Arrays.asList("use_runner_v2");
        options.setDataflowServiceOptions(serviceOptions);
        // options.setNumberOfWorkerHarnessThreads(10);
        options.setStreaming(true);
        options.setEnableStreamingEngine(true);

        LOG.info("NumberOfWorkerHarnessThreads: " + options.getNumberOfWorkerHarnessThreads());
        LOG.info("Disksize: " + options.getDiskSizeGb());
        // validateOptions(options);  // to-do create a validation function...

        Pipeline pipeline = Pipeline.create(options);

        /*
         * Create a coder that can seriealize rows with different schemas
         */
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();

        FailsafeElementCoder<PubsubMessage, PubsubMessage> pubsubFailsafeElementCoder =
                FailsafeElementCoder.of(
                        PubsubMessageWithAttributesCoder.of(),
                        NullableCoder.of(PubsubMessageWithAttributesCoder.of()));

        FailsafeElementCoder<PubsubMessage, Row> rowFailsafeElementCoder =
                FailsafeElementCoder.of(
                        PubsubMessageWithAttributesCoder.of(),
                        NullableCoder.of(SerializableCoder.of(Row.class)));

        GenericRowCoder rowCoder = new GenericRowCoder();

        coderRegistry.registerCoderForClass(FailsafeElement.class, pubsubFailsafeElementCoder);
        coderRegistry.registerCoderForClass(FailsafeElement.class, rowFailsafeElementCoder);
        coderRegistry.registerCoderForClass(Row.class, rowCoder);

        PCollection<PubsubMessage> pubsubMessagesCollection =
                pipeline.apply(
                        "Read Json pubsub messages",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(options.getInputSubscription()));

        PCollectionTuple enrichedMessages =
                pubsubMessagesCollection.apply(
                        "Transform message stream change events",
                        ParDo.of(
                                        new TransformMessageFn(
                                                options.getJobName(),
                                                options.getVersion(),
                                                options.getDataContractsServiceUrl(),
                                                TRANSFORM_MESSAGE_SUCCESS_TAG,
                                                TRANSFORM_MESSAGE_FAILURE_TAG))
                                .withOutputTags(
                                        TRANSFORM_MESSAGE_SUCCESS_TAG,
                                        TupleTagList.of(TRANSFORM_MESSAGE_FAILURE_TAG)));

        enrichedMessages.get(TRANSFORM_MESSAGE_SUCCESS_TAG).setCoder(pubsubFailsafeElementCoder);
        enrichedMessages.get(TRANSFORM_MESSAGE_FAILURE_TAG).setCoder(pubsubFailsafeElementCoder);

        PCollectionTuple serialized =
                enrichedMessages
                        .get(TRANSFORM_MESSAGE_SUCCESS_TAG)
                        .apply(
                                "Serialize to Rows",
                                ParDo.of(
                                                new SerializeMessageToRowFn(
                                                        SERIALIZED_SUCCESS_TAG,
                                                        SERIALIZED_FAILURE_TAG,
                                                        options.getJobName(),
                                                        options.getProject(),
                                                        options.getDataContractsServiceUrl(),
                                                        options.getSchemaCheckRatio()))
                                        .withOutputTags(
                                                SERIALIZED_SUCCESS_TAG,
                                                TupleTagList.of(SERIALIZED_FAILURE_TAG)));

        serialized.get(SERIALIZED_SUCCESS_TAG).setCoder(rowFailsafeElementCoder);
        serialized.get(SERIALIZED_FAILURE_TAG).setCoder(rowFailsafeElementCoder);

        PCollectionTuple tokenized =
                serialized
                        .get(SERIALIZED_SUCCESS_TAG)
                        .apply(
                                "De-identify Rows",
                                ParDo.of(
                                                new DeIdentifyFn(
                                                        options.getJobName(),
                                                        options.getFirestoreProjectId(),
                                                        DEIDENTIFY_SUCCESS_TAG,
                                                        DEIDENTIFY_FAILURE_TAG))
                                        .withOutputTags(
                                                DEIDENTIFY_SUCCESS_TAG,
                                                TupleTagList.of(DEIDENTIFY_FAILURE_TAG)));

        tokenized.get(DEIDENTIFY_SUCCESS_TAG).setCoder(rowFailsafeElementCoder);
        tokenized.get(DEIDENTIFY_FAILURE_TAG).setCoder(rowFailsafeElementCoder);

        /*
         * If a BigQuery Dataset is configured, dynamically create table if not exists and name it
         * according to topic.
         */

        if (options.getDataContractsServiceUrl() != null) {
            String projectId = options.getProject();

            PCollection<Row> extractRowElement =
                    tokenized
                            .get(DEIDENTIFY_SUCCESS_TAG)
                            .apply("Extract Row element", ParDo.of(new ExtractCurrentElementFn<>()))
                            .setCoder(rowCoder);

            WriteResult result =
                    extractRowElement.apply(
                            "Write de-identified Rows to BigQuery",
                            BigQueryIO.<Row>write()
                                    .withFormatFunction(r -> BigQueryUtils.toTableRow((Row) r))
                                    .ignoreUnknownValues()
                                    .withCreateDisposition(
                                            BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                    .withWriteDisposition(
                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                    .to(SchemaDestinations.schemaDestination(projectId))
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
                                                        "DataContracts.main()",
                                                        message);
                                                return "";
                                            }));
        }

        /*
         * Publish to a common backup topic if exists
         */
        if (options.getBackupTopic() != null) {
            tokenized
                    .get(DEIDENTIFY_SUCCESS_TAG)
                    .apply(
                            "Transform Rows to Pubsub Messages",
                            ParDo.of(new RowToPubsubMessageFn()))
                    .apply("Get the pubsub messages", Values.create())
                    .apply(
                            "Write to backup pubsub topic",
                            PubsubIO.writeMessages().to(options.getBackupTopic()));
        }

        /*
         * Write failed elements out to dead letter topic if dead letter topic is set
         */
        if (options.getDeadLetterTopic() != null) {
            // Failed elements of type PubsubMessage
            PCollection<PubsubMessage> failedPubsubElements =
                    enrichedMessages
                            .get(TRANSFORM_MESSAGE_FAILURE_TAG)
                            .apply(
                                    "Failed Pubsub elements",
                                    ParDo.of(
                                            new FailSafeElementToPubsubMessageFn<PubsubMessage>()));

            // Failed elements of type Row
            PCollection<PubsubMessage> failedRowElements =
                    PCollectionList.of(serialized.get(SERIALIZED_FAILURE_TAG))
                            .and(tokenized.get(DEIDENTIFY_FAILURE_TAG))
                            .apply("Collect failed Row elements", Flatten.pCollections())
                            .apply(
                                    "Failed Row elements",
                                    ParDo.of(new FailSafeElementToPubsubMessageFn<Row>()));

            PCollectionList.of(failedPubsubElements)
                    .and(failedRowElements)
                    .apply("Failure collector", Flatten.pCollections())
                    .apply(
                            "Write to dead-letter pubsub topic",
                            PubsubIO.writeMessages().to(options.getDeadLetterTopic()));
        }

        pipeline.run();
    }
}
