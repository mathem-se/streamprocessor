package org.streamprocessor.core.helpers;

import java.util.HashMap;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class MySQLDataStreamHelper {

    public static final String METADATA = "_metadata";
    public static final String UUID = "uuid";
    public static final String INSERT = "INSERT";
    public static final String DELETE = "DELETE";
    public static final String UPDATE_INSERT = "UPDATE-INSERT";
    public static final String CHANGE_TYPE = "change_type";
    public static final String SOURCE_METADATA = "source_metadata";
    public static final String PAYLOAD = "payload";
    public static final String SOURCE_TIMESTAMP = "source_timestamp";

    public static JSONObject enrichPubsubMessage(
            JSONObject mysqlDataStreamStreamObject, HashMap<String, String> attributes)
            throws Exception {

        JSONObject metadata = mysqlDataStreamStreamObject.getJSONObject(METADATA);

        JSONObject sourceMetadata = mysqlDataStreamStreamObject.getJSONObject(SOURCE_METADATA);
        String changeType = sourceMetadata.getString(CHANGE_TYPE);
        JSONObject payloadObject = mysqlDataStreamStreamObject.getJSONObject(PAYLOAD);

        if (changeType.equals(INSERT)) {
            metadata.put(MetadataFields.OPERATION, MetadataFields.Operation.INSERT.getValue());
        } else if (changeType.equals(DELETE)) {
            metadata.put(MetadataFields.OPERATION, MetadataFields.Operation.REMOVE.getValue());
        } else if (changeType.equals(UPDATE_INSERT)) {
            metadata.put(MetadataFields.OPERATION, MetadataFields.Operation.MODIFY.getValue());
        } else {
            throw new CustomExceptionsUtils.MalformedEventException(
                    String.format(
                            "Unknown %s: `%s` found in message. Maybe the provider is not"
                                    + " configured correctly?",
                            CHANGE_TYPE, changeType));
        }

        metadata.put(MetadataFields.EXTRACT_METHOD, MetadataFields.ExtractMethod.CDC.getValue());
        metadata.put(MetadataFields.LOG_POSITION, sourceMetadata.getString(MetadataFields.LOG_POSITION));
        metadata.put(MetadataFields.LOG_FILE, sourceMetadata.getString(MetadataFields.LOG_FILE));

        // add event_time to payload root for streaming analytics use cases
        payloadObject.put(
                MetadataFields.EVENT_TIMESTAMP,
                mysqlDataStreamStreamObject.getString(SOURCE_TIMESTAMP));

        // Add meta-data from dynamoDB stream event as attributes
        metadata.put(MetadataFields.EVENT_ID, mysqlDataStreamStreamObject.getString(UUID));

        payloadObject.put(METADATA, metadata);
        return payloadObject;
    }
}
