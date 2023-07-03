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
            JSONObject customEventStreamObject,
            HashMap<String, String> attributes,
            HashMap<String, String> newAttributes)
            throws Exception {
        newAttributes.put(
                MetadataFields.Operation.OPERATION.getValue(),
                MetadataFields.Operation.INSERT.getValue());

        newAttributes.put(
                MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(),
                MetadataFields.ExtractMethod.CUSTOM_EVENT.getValue());

        // add event_time to payload root for streaming analytics use cases
        if (customEventStreamObject.isNull(MetadataFields.Event.EVENT_TIMESTAMP.getValue())) {
            if (attributes.containsKey(TIMESTAMP)) {
                customEventStreamObject.put(
                        MetadataFields.Event.EVENT_TIMESTAMP.getValue(), attributes.get(TIMESTAMP));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from custom events as attributes
        if (!customEventStreamObject.isNull(MetadataFields.Event.EVENT_ID.getValue())) {
            newAttributes.put(
                    MetadataFields.Event.EVENT_ID.getValue(),
                    customEventStreamObject.getString(MetadataFields.Event.EVENT_ID.getValue()));
        } else if (attributes.containsKey(UUID)) {
            newAttributes.put(MetadataFields.Event.EVENT_ID.getValue(), attributes.get(UUID));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `event_id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                customEventStreamObject.toString().getBytes(StandardCharsets.UTF_8), newAttributes);
    }
}
