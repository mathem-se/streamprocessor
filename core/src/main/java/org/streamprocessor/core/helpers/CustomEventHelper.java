package org.streamprocessor.core.helpers;

import java.util.HashMap;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class CustomEventHelper {

    public static final String METADATA = "_metadata";
    public static final String TIMESTAMP = "timestamp";
    public static final String UUID = "uuid";

    public static JSONObject enrichPubsubMessage(
            JSONObject customEventStreamObject, HashMap<String, String> attributes)
            throws Exception {

        JSONObject metadata = customEventStreamObject.getJSONObject(METADATA);
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
                        String.format("No `%s` found in message", MetadataFields.EVENT_TIMESTAMP));
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
                    String.format(
                            "No `%s` or `%s` found in message.", MetadataFields.EVENT_ID, UUID));
        }
        customEventStreamObject.put(METADATA, metadata);
        return customEventStreamObject;
    }
}
