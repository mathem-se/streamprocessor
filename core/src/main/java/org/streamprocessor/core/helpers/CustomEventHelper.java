package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class CustomEventHelper {

    public static PubsubMessage enrichPubsubMessage(
            JSONObject customEventStreamObject, HashMap<String, String> attributes, HashMap<String, String> newAttributes)
            throws Exception {

        // TODO need to set in newAttributes:
        //          event_id
        //          operation (INSERT, REMOVE, MODIFY)
        //          extract_method
        //          event_timestamp

        // add event_time to payload root for streaming analytics use cases
        if (customEventStreamObject.isNull("event_timestamp")) {
            if (attributes.containsKey("timestamp")) {
                customEventStreamObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from custom events as attributes
        if (!customEventStreamObject.isNull("event_id")) {
            attributes.put("event_id", customEventStreamObject.getString("event_id"));
        } else if (attributes.containsKey("uuid")) {
            attributes.put("event_id", attributes.get("uuid"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `event_id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                customEventStreamObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
