package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class CustomEventHelper {

    public static final String TIMESTAMP = "timestamp";
    public static final String UUID = "uuid";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject customEventStreamObject, HashMap<String, String> attributes)
            throws Exception {

        JSONObject metadata = new JSONObject(customEventStreamObject.get("_metadata"));
        metadata.put(MetadataFields.OPERATION, MetadataFields.Operation.INSERT.getValue());

        metadata.put(
                MetadataFields.EXTRACT_METHOD,
                MetadataFields.ExtractMethod.CUSTOM_EVENT.getValue());

        // add event_time to payload root for streaming analytics use cases
        if (customEventStreamObject.isNull(MetadataFields.EVENT_TIMESTAMP)) {
            if (attributes.containsKey(TIMESTAMP)) {
                customEventStreamObject.put(
                        MetadataFields.EVENT_TIMESTAMP, attributes.get(TIMESTAMP));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from custom events as attributes
        if (!customEventStreamObject.isNull(MetadataFields.EVENT_ID)) {
            metadata.put(
                    MetadataFields.EVENT_ID,
                    customEventStreamObject.getString(MetadataFields.EVENT_ID));
        } else if (attributes.containsKey(UUID)) {
            metadata.put(MetadataFields.EVENT_ID, attributes.get(UUID));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `event_id` or `uuid` found in message.");
        }
        customEventStreamObject.put("_metadata", metadata);
        return new PubsubMessage(
                customEventStreamObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
