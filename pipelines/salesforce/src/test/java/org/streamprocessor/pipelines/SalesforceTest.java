package org.streamprocessor.pipelines;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
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
import org.streamprocessor.core.transforms.SalesforceFn;

/** Test cases for the {@link Salesforce} class. */
public class SalesforceTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    /** Tests the {@link Salesforce} pipeline. */
    @Test
    public void testCorrectPayload() throws Exception {
        // Test input
        final String payload =
                "{\"detail\": {\"id\": 123}, \"time\": \"2021-02-22T22:22:22.222Z\", \"EventId\":"
                        + " \"123\"}";
        final PubsubMessage message =
                new PubsubMessage(
                        payload.getBytes(), ImmutableMap.of("uuid", "111", "topic", "salesforce"));

        final Instant timestamp =
                new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        // Build pipeline
        PCollection<PubsubMessage> transformOut =
                pipeline.apply(
                                "CreateInput",
                                Create.timestamped(TimestampedValue.of(message, timestamp))
                                        .withCoder(PubsubMessageWithAttributesCoder.of()))
                        .apply(
                                "Transform salesforce stream change events",
                                ParDo.of(new SalesforceFn()));

        PAssert.that(transformOut)
                .satisfies(
                        item -> {
                            PubsubMessage result = item.iterator().next();
                            String resultPayload =
                                    new String(result.getPayload(), StandardCharsets.UTF_8);
                            JSONObject resultPayloadJSON = new JSONObject(resultPayload);
                            Integer test = (Integer) resultPayloadJSON.get("id");
                            assertEquals(test, 123);
                            assertEquals(
                                    resultPayloadJSON.get("event_timestamp"),
                                    "2021-02-22T22:22:22.222Z");
                            return null;
                        });
        // Execute pipeline
        pipeline.run();
    }
}
