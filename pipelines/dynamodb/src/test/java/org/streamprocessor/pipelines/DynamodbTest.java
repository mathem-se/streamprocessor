package org.streamprocessor.pipelines;

import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.streamprocessor.core.transforms.DynamodbFn;

/** Test cases for the {@link Dynamodb} class. */
public class DynamodbTest {
    
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests the {@link Dynamodb} pipeline. */
  @Test
  public void testWrongPayload() throws Exception {
    // Test input
    final String eventTimestamp = "2021-02-22T22:22:22.222Z";
    final String payload = "{\"NewImage\": {\"id\": 123}, \"test\": " + eventTimestamp + ", \"EventId\": \"123\"}";
    final PubsubMessage message =
        new PubsubMessage(payload.getBytes(), ImmutableMap.of("uuid", "111"));

    final Instant timestamp =
        new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

    // Build pipeline
    PCollection<PubsubMessage> transformOut =
        pipeline
            .apply(
                "CreateInput",
                Create.timestamped(TimestampedValue.of(message, timestamp))
                    .withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply("Transform Dynamodb stream change events", ParDo.of(new DynamodbFn()));

    // Assert
    PAssert.that(transformOut).empty();
    // Execute pipeline
    pipeline.run();
  }

  /** Tests the {@link Dynamodb} pipeline. */
  @Test
  public void testCorrectPayload() throws Exception {
    // Test input
    final String payload = "{\"NewImage\": {\"id\": 123}, \"Published\": \"2021-02-22T22:22:22.222Z\", \"EventId\": \"123\"}";
    final PubsubMessage message =
        new PubsubMessage(payload.getBytes(), ImmutableMap.of("uuid", "111"));

    final Instant timestamp =
        new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

    // Build pipeline
    PCollection<PubsubMessage> transformOut =
        pipeline
            .apply(
                "CreateInput",
                Create.timestamped(TimestampedValue.of(message, timestamp))
                    .withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply("Transform Dynamodb stream change events", ParDo.of(new DynamodbFn()));

    PAssert.that(transformOut).satisfies(item -> {
        PubsubMessage result = item.iterator().next();
        String resultPayload = new String(result.getPayload(), StandardCharsets.UTF_8);
        JSONObject resultPayloadJSON = new JSONObject(resultPayload);
        Integer test = (Integer) resultPayloadJSON.get("id");
        assertEquals(test, 123);
        assertEquals(resultPayloadJSON.get("event_timestamp"), "2021-02-22T22:22:22.222Z");
        return null;
    });
    // Execute pipeline
    pipeline.run();
  }

}
