package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class DynamodbHelper {

    public static final String OLD_IMAGE = "OldImage";
    public static final String NEW_IMAGE = "NewImage";
    public static final String PUBLISHED = "Published";
    public static final String TIMESTAMP = "timestamp";
    public static final String EVENT_ID = "EventId";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject dynamodbStreamObject,
            HashMap<String, String> attributes,
            HashMap<String, String> newAttributes)
            throws Exception {
        JSONObject payloadObject;

        if ((dynamodbStreamObject.isNull(OLD_IMAGE)
                        || dynamodbStreamObject.getJSONObject(OLD_IMAGE).isEmpty())
                && dynamodbStreamObject.has(NEW_IMAGE)) {
            newAttributes.put(
                    MetadataFields.EXTRACT_METHOD, MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.INSERT.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(NEW_IMAGE);
        } else if ((dynamodbStreamObject.isNull(NEW_IMAGE)
                        || dynamodbStreamObject.getJSONObject(NEW_IMAGE).isEmpty())
                && dynamodbStreamObject.has(OLD_IMAGE)) {
            newAttributes.put(
                    MetadataFields.EXTRACT_METHOD, MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.REMOVE.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(OLD_IMAGE);
        } else if (dynamodbStreamObject.has(NEW_IMAGE) && dynamodbStreamObject.has(OLD_IMAGE)) {
            newAttributes.put(
                    MetadataFields.EXTRACT_METHOD, MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.MODIFY.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(NEW_IMAGE);
        } else {
            throw new CustomExceptionsUtils.MalformedEventException(
                    "No NewImage or OldImage found in message. Maybe the provider is not configured"
                            + " correctly?");
        }

        // add event_time to payload root for streaming analytics use cases
        if (dynamodbStreamObject.isNull(MetadataFields.EVENT_TIMESTAMP)) {
            if (!dynamodbStreamObject.isNull(PUBLISHED)) {
                payloadObject.put(
                        MetadataFields.EVENT_TIMESTAMP, dynamodbStreamObject.getString(PUBLISHED));
                // Used for backfill purposes
            } else if (attributes.containsKey(TIMESTAMP)) {
                payloadObject.put(MetadataFields.EVENT_TIMESTAMP, attributes.get(TIMESTAMP));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull(EVENT_ID)) {
            newAttributes.put(MetadataFields.EVENT_ID, dynamodbStreamObject.getString(EVENT_ID));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `EventId` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), newAttributes);
    }
}
