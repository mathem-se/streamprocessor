package org.streamprocessor.core.helpers;

import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class DynamodbHelper {
    private static class MissingMetadataException extends Exception {
        private MissingMetadataException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static PubsubMessage enrichPubsubMessage(
            JSONObject dynamodbStreamObject, HashMap<String, String> attributes) throws Exception {
        JSONObject payloadObject;

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
        } else if (dynamodbStreamObject.has("NewImage") && dynamodbStreamObject.has("OldImage")) {
            attributes.put("operation", "MODIFY");
            payloadObject = dynamodbStreamObject.getJSONObject("NewImage");
        } else {
            // Not a dynamoDB change event
            payloadObject = dynamodbStreamObject;
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull("Published")) {
            attributes.put("dynamodbPublished", dynamodbStreamObject.getString("Published"));
        } else if (attributes.containsKey("timestamp")) {
            attributes.put("dynamodbPublished", attributes.get("timestamp"));
        } else {
            throw new MissingMetadataException("No published found in message");
        }

        // add event_time to payload root for streaming analytics use cases
        if (dynamodbStreamObject.isNull("event_timestamp")) {
            if (!dynamodbStreamObject.isNull("Published")) {
                payloadObject.put("event_timestamp", dynamodbStreamObject.getString("Published"));
            } else if (attributes.containsKey("timestamp")) {
                payloadObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new MissingMetadataException("No event_timestamp found in message");
            }
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull("EventId")) {
            attributes.put("dynamodbEventId", dynamodbStreamObject.getString("EventId"));
            // Add meta-data from custom events as attributes
        } else if (!dynamodbStreamObject.isNull("event_id")) {
            attributes.put("event_id", payloadObject.getString("event_id"));
        } else if (attributes.containsKey("uuid")) {
            attributes.put("event_id", attributes.get("uuid"));
        } else {
            throw new MissingMetadataException("No event_id found in message with uuid");
        }

        return new PubsubMessage(payloadObject.toString().getBytes("UTF-8"), attributes);
    }
}
