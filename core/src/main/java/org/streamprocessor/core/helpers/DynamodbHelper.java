package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class DynamodbHelper {

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
            throw new CustomExceptionsUtils.MalformedEventException(
                    "No NewImage or OldImage found in message. Maybe the provider is not configured"
                            + " correctly?");
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull("Published")) {
            attributes.put("dynamodbPublished", dynamodbStreamObject.getString("Published"));
            // Used for backfill purposes
        } else if (attributes.containsKey("timestamp")) {
            attributes.put("dynamodbPublished", attributes.get("timestamp"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `published` found in message");
        }

        // add event_time to payload root for streaming analytics use cases
        if (dynamodbStreamObject.isNull("event_timestamp")) {
            if (!dynamodbStreamObject.isNull("Published")) {
                payloadObject.put("event_timestamp", dynamodbStreamObject.getString("Published"));
                // Used for backfill purposes
            } else if (attributes.containsKey("timestamp")) {
                payloadObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull("EventId")) {
            attributes.put("dynamodbEventId", dynamodbStreamObject.getString("EventId"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `EventId` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
