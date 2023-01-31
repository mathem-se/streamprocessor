/* (C)2021 */
package org.streamprocessor.core.transforms;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamodbFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamodbFn.class);
    static final long serialVersionUID = 238L;

    String unknownFieldLogger;
    String format;

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    public DynamodbFn() {}

    @ProcessElement
    public void processElement(@Element PubsubMessage received, OutputReceiver<PubsubMessage> out)
            throws Exception {
        try {
            String receivedPayload = new String(received.getPayload(), StandardCharsets.UTF_8);
            JSONObject dynamodbStreamObject = new JSONObject(receivedPayload);

            HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.putAll(received.getAttributeMap());
            attributes.put(
                    "processing_timestamp",
                    Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

            // use topic attribute as entity attribute if entity is missing
            if (received.getAttribute("entity") == null && received.getAttribute("topic") != null) {
                attributes.put("entity", received.getAttribute("topic"));
            }

            JSONObject payloadObject;

            try {
                // add operation and payload according to dynamodb 'NEW_AND_OLD_IMAGES' stream view
                // type
                if ((dynamodbStreamObject.isNull("OldImage")
                                || dynamodbStreamObject.getJSONObject("OldImage").isEmpty())
                        && dynamodbStreamObject.has("NewImage")) {
                    attributes.put("operation", "INSERT");
                    payloadObject = dynamodbStreamObject.getJSONObject("NewImage");
                } else if ((dynamodbStreamObject.isNull("NewImage")
                                || dynamodbStreamObject.getJSONObject("NewImage").isEmpty())
                        && dynamodbStreamObject.has("OldImage")) {
                    attributes.put("operation", "REMOVE");
                    payloadObject = dynamodbStreamObject.getJSONObject("OldImage");
                } else if (dynamodbStreamObject.has("NewImage")
                        && dynamodbStreamObject.has("OldImage")) {
                    attributes.put("operation", "MODIFY");
                    payloadObject = dynamodbStreamObject.getJSONObject("NewImage");
                } else {
                    // Not a dynamoDB change event
                    payloadObject = dynamodbStreamObject;
                }

                // Add meta-data from dynamoDB stream event as attributes
                if (!dynamodbStreamObject.isNull("Published")) {
                    attributes.put(
                            "dynamodbPublished", dynamodbStreamObject.getString("Published"));
                }

                // add event_time to payload root for streaming analytics use cases
                if (dynamodbStreamObject.isNull("event_timestamp")) {
                    if (!dynamodbStreamObject.isNull("Published")) {
                        payloadObject.put(
                                "event_timestamp", dynamodbStreamObject.getString("Published"));
                    } else {
                        payloadObject.put(
                                "event_timestamp",
                                DateTime.now().withZone(DateTimeZone.UTC).toString());
                    }
                }

                // Add meta-data from dynamoDB stream event as attributes
                if (!dynamodbStreamObject.isNull("EventId")) {
                    attributes.put("dynamodbEventId", dynamodbStreamObject.getString("EventId"));
                }

                out.output(
                        new PubsubMessage(payloadObject.toString().getBytes("UTF-8"), attributes));
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException: ", e);
            } catch (org.json.JSONException e) {
                LOG.error("org.json.JSONException: ", e);
            } catch (Exception e) {
                LOG.error("Exception: ", e);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
