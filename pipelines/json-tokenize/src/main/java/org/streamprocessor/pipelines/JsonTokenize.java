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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.coders.GenericRowCoder;
import org.streamprocessor.core.io.PublisherFn;
import org.streamprocessor.core.io.SchemaDestinations;
import org.streamprocessor.core.transforms.DeIdentifyFn;
import org.streamprocessor.core.transforms.RowToPubsubMessageFn;
import org.streamprocessor.core.transforms.SerializeMessageToRowFn;

/**
 * Streamer pipeline
 *
 * <p>Receives pub/sub messages with Json payload and serialize them to beam rows using data catalog schemas
 * and then tokenize fields based on tags and then writes to BigQuery and pub/sub topic.</p>
 *
 * Stages:
 *   1) Read JSON from Pubsub subscription
 *   2) Serialize to Beam Rows using Data Catalog schema
 *   3) De-identify fields using tokenization on tagged fields
 *      Tokens are stored in Firestore
 *   4) Write to BigQuery (patch schema if needed)
 *   5) Write to Pubsub topics for streaming analytics
 *   6) Write to Pubsub backup topic
 *   7) Write Failures to Pubsub Dead Letter Queue
 *
 * @author Robert Sahlin
 * @version 1.0
 * @since 2022-04-20
 */

public class JsonTokenize {

  private static final Logger LOG = LoggerFactory.getLogger(JsonTokenize.class);

  static final TupleTag<Row> SERIALIZED_SUCCESS_TAG = new TupleTag<Row>(
    "Serialized success"
  ) {
    static final long serialVersionUID = 894723987432L;
  };

  static final TupleTag<PubsubMessage> SERIALIZED_DEADLETTER_TAG = new TupleTag<PubsubMessage>(
    "Serialized deadletter"
  ) {
    static final long serialVersionUID = 89472335422L;
  };

  static final TupleTag<Row> DEIDENTIFY_SUCCESS_TAG = new TupleTag<Row>(
    "deidentify success"
  ) {
    static final long serialVersionUID = 89472335672L;
  };

  static final TupleTag<Row> DEIDENTIFY_TOKENS_TAG = new TupleTag<Row>(
    "deidentify tokens"
  ) {
    static final long serialVersionUID = 89472335673L;
  };

  static final TupleTag<Row> DEIDENTIFY_FAILURE_TAG = new TupleTag<Row>(
    "deidentify failure"
  ) {
    static final long serialVersionUID = 89472335674L;
  };

  static final TupleTag<PubsubMessage> JSON_MESSAGE_TAG = new TupleTag<PubsubMessage>(
    "jsonMessage"
  ) {
    static final long serialVersionUID = 89472334422L;
  };

  //static LoadingCache<String, Schema> schemaCache;

  /**
   * Provides the custom execution options passed by the executor at the command-line.
   * <p>Inherits standard configuration options.
   */
  public interface Options extends DataflowPipelineOptions {
    @Description("Pubsub Input Subscription")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description(
      "Firestore Project Id, if other than the project where Dataflow job runs"
    )
    String getFirestoreProjectId();

    void setFirestoreProjectId(String value);

    @Description("Pubsub topic for backup of tokenized data")
    String getBackupTopic();

    void setBackupTopic(String value);

    @Description("Pubsub topic for deadletter output")
    String getDeadLetterTopic();

    void setDeadLetterTopic(String value);

    @Description("BigQuery Dataset")
    String getBigQueryDataset();

    void setBigQueryDataset(String value);

    @Description("Publish to entity topics")
    @Default.Boolean(false)
    boolean getEntityTopics();

    void setEntityTopics(boolean value);
  }

  /**
   * Main entry point. Runs a pipeline which reads data from pubsub and writes to BigQuery and pubsub.
   *
   * @param args the command line arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory
      .fromArgs(args)
      .withValidation()
      .as(Options.class);

    List<String> serviceOptions = Arrays.asList("use_runner_v2");
    options.setDataflowServiceOptions(serviceOptions);
    //options.setNumberOfWorkerHarnessThreads(10);
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);
    LOG.info(
      "NumberOfWorkerHarnessThreads: " +
      options.getNumberOfWorkerHarnessThreads()
    );

    //validateOptions(options);  //create a validation function...

    Pipeline pipeline = Pipeline.create(options);

    /*
     * Create a coder that can seriealize rows with different schemas
     */
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    //GenericRowCoder coder = new GenericRowCoder();
    GenericRowCoder coder = new GenericRowCoder(
      options.getProject(),
      options.getBigQueryDataset()
    );
    coderRegistry.registerCoderForClass(Row.class, coder);

    /*
     * Read Pub/Sub messages with Json payload and serialize to Beam Rows
     */
    PCollectionTuple serialized = pipeline
      .apply(
        "Read Json pubsub messages",
        PubsubIO
          .readMessagesWithAttributes()
          .fromSubscription(options.getInputSubscription())
      )
      .apply(
        "Serialize to Rows",
        ParDo
          .of(
            new SerializeMessageToRowFn(
              SERIALIZED_SUCCESS_TAG,
              SERIALIZED_DEADLETTER_TAG,
              options.getProject(),
              options.getBigQueryDataset()
            )
          )
          .withOutputTags(
            SERIALIZED_SUCCESS_TAG,
            TupleTagList.of(SERIALIZED_DEADLETTER_TAG)
          )
      );

    /*
     * Publish deadletter to pubsub topic if exists
     */
    if (options.getDeadLetterTopic() != null) {
      serialized
        .get(SERIALIZED_DEADLETTER_TAG)
        .apply(
          "Write to deadLetter pubsub topic",
          PubsubIO.writeMessages().to(options.getDeadLetterTopic())
        );
    }

    PCollection<Row> tokenized = serialized
      .get(SERIALIZED_SUCCESS_TAG)
      .setCoder(coder)
      .apply(
        "De-identify Rows",
        ParDo.of(new DeIdentifyFn(options.getFirestoreProjectId()))
      )
      .setCoder(coder);

    //PCollection<Row> failures = deIdentified.get(DEIDENTIFY_FAILURE_TAG).setCoder(coder);
    //PCollection<Row> tokens = deIdentified.get(DEIDENTIFY_TOKENS_TAG).setCoder(coder);
    //PCollection<Row> tokenized = deIdentified.get(DEIDENTIFY_SUCCESS_TAG).setCoder(coder);

    /*
     * If a BigQuery Dataset is configured, dynamically create table if not exists and name it
     * according to topic.
     */

    if (options.getBigQueryDataset() != null) {
      String projectId = options.getProject();
      String bigQueryDataset = options.getBigQueryDataset();

      tokenized.apply(
        "Write de-identified Rows to BigQuery",
        BigQueryIO
          .<Row>write()
          .withFormatFunction(r -> BigQueryUtils.toTableRow((Row) r))
          .ignoreUnknownValues()
          .withCreateDisposition(
            BigQueryIO.Write.CreateDisposition.CREATE_NEVER
          )
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .to(SchemaDestinations.schemaDestination(projectId, bigQueryDataset))
          .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        //.withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE) //https://issues.apache.org/jira/browse/BEAM-13954
        //.withAutoSharding()
      );
    }

    /*
     * Transform tokenized rows to pubsub messages and fan out to multiple topics for streaming analytics
     */
    if (options.getEntityTopics() || options.getBackupTopic() != null) {
      PCollection<KV<String, PubsubMessage>> pubsubMessages = tokenized.apply(
        "Transform Rows to Pubsub Messages",
        ParDo.of(new RowToPubsubMessageFn())
      );

      /*
       * Fan out to multiple topics for streaming analytics
       */
      if (options.getEntityTopics()) {
        pubsubMessages
          .apply(
            "Fixed Windows",
            Window
              .<KV<String, PubsubMessage>>into(
                FixedWindows.of(Duration.standardSeconds(5))
              )
              .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
              .withAllowedLateness(Duration.standardMinutes(1))
              .discardingFiredPanes()
          )
          .apply(
            "Group messages by topic",
            GroupIntoBatches
              .<String, PubsubMessage>ofSize(1000L)
              .withMaxBufferingDuration(Duration.standardSeconds(5))
          )
          .apply("Publish on topics", ParDo.of(new PublisherFn()));
      }

      /*
       * Publish to a common backup topic if exists
       */
      if (options.getBackupTopic() != null) {
        pubsubMessages
          .apply("Get the pubsub messages", Values.create())
          .apply(
            "Write to backup pubsub topic",
            PubsubIO.writeMessages().to(options.getBackupTopic())
          );
      }
    }

    pipeline.run();
  }
}
