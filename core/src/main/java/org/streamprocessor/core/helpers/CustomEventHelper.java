package org.streamprocessor.core.helpers;

import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class CustomEventHelper {
    private static class MissingMetadataException extends Exception {
        private MissingMetadataException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static PubsubMessage enrichPubsubMessage(
            JSONObject customEventStreamObject, HashMap<String, String> attributes)
            throws Exception {

        // add event_time to payload root for streaming analytics use cases
        if (customEventStreamObject.isNull("event_timestamp")) {
            if (attributes.containsKey("timestamp")) {
                customEventStreamObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new MissingMetadataException("No event_timestamp found in message");
            }
        }

        // Add meta-data from custom events as attributes
        if (!customEventStreamObject.isNull("event_id")) {
            attributes.put("event_id", customEventStreamObject.getString("event_id"));
        } else if (attributes.containsKey("uuid")) {
            attributes.put("event_id", attributes.get("uuid"));
        } else {
            throw new MissingMetadataException("No event_id found in message with uuid");
        }

        return new PubsubMessage(customEventStreamObject.toString().getBytes("UTF-8"), attributes);
    }
}
