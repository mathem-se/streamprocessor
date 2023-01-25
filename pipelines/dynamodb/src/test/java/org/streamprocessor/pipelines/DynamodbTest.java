package org.streamprocessor.pipelines;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.coders.GenericRowCoder;
import org.streamprocessor.core.io.PublisherFn;
import org.streamprocessor.core.io.SchemaDestinations;
import org.streamprocessor.core.transforms.DeIdentifyFn;
import org.streamprocessor.core.transforms.RowToPubsubMessageFn;
import org.streamprocessor.core.transforms.SerializeMessageToRowFn;
import org.streamprocessor.core.utils.CacheLoaderUtils;

@RunWith(JUnit4.class)
public class DynamodbTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DynamodbTest.class);
  static final TupleTag<Row> SERIALIZED_SUCCESS_TAG = new TupleTag<Row>("serialization success") {};
  static final TupleTag<PubsubMessage> SERIALIZED_DEADLETTER_TAG = new TupleTag<PubsubMessage>("serialization deadletter") {};
  static final TupleTag<Row> DEIDENTIFY_SUCCESS_TAG = new TupleTag<Row>("deidentify success") {};
  static final TupleTag<Row> DEIDENTIFY_TOKENS_TAG = new TupleTag<Row>("deidentify tokens") {};
  static final TupleTag<Row> DEIDENTIFY_FAILURE_TAG = new TupleTag<Row>("deidentify failure") {};

  public static <T> T getValueOrDefault(T value, T defaultValue) {
    return value == null ? defaultValue : value;
  }

  @Rule public transient TestPipeline p = TestPipeline.create();

  static LoadingCache<String, Schema> schemaCache =
  CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(60, TimeUnit.SECONDS)
      .build(CacheLoaderUtils.schemaCacheLoader());


  @Test
    public void withoutOptionsTest() {

      JSONObject testPayloadObject =
          new JSONObject()
              .put("firstname", "Joe")
              .put("lastname", "Doe")
              .put("email", "john.doe@gmail.com")
              .put("age", 23)
              .put("latitude", 59.334591)
          ;

      Map<String, String> attributes =
          new HashMap<String, String>() {
            {
              put("timestamp", "2013-08-16T23:36:32.444Z");
              put("uuid", "123-456-abc");
              put("topic", "person");
            }
          };

      String testPayload = testPayloadObject.toString();
      byte[] payload = testPayload.getBytes(StandardCharsets.UTF_8);
      PubsubMessage pm = new PubsubMessage(payload, attributes);

      Schema schema =
          new Schema.Builder()
              .addField(
                  Schema.Field.of("firstname", Schema.FieldType.STRING)
                      .withDescription("test description")
                      .withOptions(
                          Schema.Options.builder()
                              .setOption("replaceAllRegex", Schema.FieldType.STRING, "(e)")
                              .setOption("replaceAllReplacement", Schema.FieldType.STRING, "a")
                              .build()))
              .addField(Schema.Field.nullable("lastname", Schema.FieldType.STRING).withDescription("test"))
              .addField(Schema.Field.nullable("email", Schema.FieldType.STRING))
              .addField(Schema.Field.nullable("age", Schema.FieldType.INT32))
              .addField(Schema.Field.nullable("latitude", Schema.FieldType.DOUBLE))
              .build()
              .withOptions(
                  Schema.Options.builder()
                      .setOption("topic", Schema.FieldType.STRING, "person")
                      .setOption("timePartitioningType", Schema.FieldType.STRING, "MONTH")
                      .setOption("tableId", Schema.FieldType.STRING, "person2")
                      .build());
      LOG.info("assertSchema: " + schema.toString());

      Row assertRow =
          Row.withSchema(schema)
              .withFieldValue("firstname", "Joe")
              .withFieldValue("lastname", "Doe")
              .withFieldValue("email", "john.doe@gmail.com")
              .withFieldValue("age", 23)
              .build();

      LOG.info("assertRow: " + assertRow);

      try {
        CoderRegistry coderRegistry = p.getCoderRegistry();
        GenericRowCoder coder = new GenericRowCoder();
        coderRegistry.registerCoderForClass(Row.class, coder);

        LOG.info("ok 1");

        PCollectionTuple serialized =
          p.apply("Mock pubsub messages", 
            Create.of(Arrays.asList(pm)).withCoder(new PubsubMessageWithAttributesCoder()))
              .apply("Serialize to Rows",
                ParDo.of(new SerializeMessageToRowFn(SERIALIZED_SUCCESS_TAG, SERIALIZED_DEADLETTER_TAG, "streamprocessor-demo", "test"))
                  .withOutputTags(SERIALIZED_SUCCESS_TAG, TupleTagList.of(SERIALIZED_DEADLETTER_TAG)));

        LOG.info("ok 2");

        //PCollection<PubsubMessage> deadletters = serialized.get(DEADLETTER_TAG);
        PCollection<Row> rows = serialized.get(SERIALIZED_SUCCESS_TAG).setCoder(coder);

        String projectId = "streamprocessor-demo";
        
        PCollection<Row> tokenized = rows
          .setCoder(coder)
          .apply("De-identify Rows",
            ParDo.of(new DeIdentifyFn(null)));

        //Duration triggeringFrequency = new Duration(60000L);


        String bigQueryDataset = "test";
        String bigQueryWriteMethod = "FILE_LOADS";
        ValueProvider.StaticValueProvider<String> customGcsTempLocation = ValueProvider.StaticValueProvider.of("gs://streamprocessor-gcs-temp");
      
        BigQueryIO.Write<Row> bigQueryWrite = BigQueryIO.write();
        bigQueryWrite =
            BigQueryIO.<Row>write()
                .withFormatFunction(r -> BigQueryUtils.toTableRow((Row) r))
                .ignoreUnknownValues()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .to(SchemaDestinations.schemaDestination(projectId, bigQueryDataset));
      
        if (bigQueryWriteMethod.equals("FILE_LOADS")) {
          LOG.info("write to bigquery");
          // FILE_UPLOADS supports schema updates, i.e. schema evolution in the same table.
          Set<BigQueryIO.Write.SchemaUpdateOption> updateOptions =
              new HashSet<>(Arrays.asList(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION));

          bigQueryWrite =
              bigQueryWrite
                  .withSchemaUpdateOptions(updateOptions)
                  .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                  .withCustomGcsTempLocation(customGcsTempLocation)
                  //.withTriggeringFrequency(triggeringFrequency)
                  ;
          // .withAutoSharding();
        } else if (bigQueryWriteMethod.equals("STORAGE_WRITE_API")) {
          bigQueryWrite =
              bigQueryWrite
                  //.withTriggeringFrequency(triggeringFrequency)
                  .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API);
        } else {
          bigQueryWrite = bigQueryWrite.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);
          // .withAutoSharding();
        }
      
        
        tokenized
          .setCoder(coder)
          .apply("Write de-identified Rows to BigQuery", bigQueryWrite);
        
        PCollection<KV<String,PubsubMessage>> messages = tokenized
          .apply("Transform Rows to Pubsub Messages",
            ParDo.of(new RowToPubsubMessageFn()));

        messages
          .apply("Group messages by topic", GroupByKey.<String, PubsubMessage>create())
          .apply("Publish on topics", ParDo.of(new PublisherFn()));
        
        //messages.apply(ParDo.of(new DynamicPublishFn()));

        //PCollection<Row> output = rows.get(SUCCESS_TAG);
        //PAssert.that(rows).containsInAnyOrder(assertRow);
        p.run();
        LOG.info("withoutOptionsTest assert TableRow without errors.");
      } catch (Exception e) {
        LOG.error(e.getMessage());
        e.printStackTrace();
      }
    }
}