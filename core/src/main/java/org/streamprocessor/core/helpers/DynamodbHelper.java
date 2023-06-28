package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;


public class DynamodbHelper {

    public static final String oldImage = "OldImage";
    public static final String newImage = "NewImage";
    public static final String published = "Published";
    public static final String timestamp = "timestamp";
    public static final String eventId = "EventId";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject dynamodbStreamObject, HashMap<String, String> attributes, HashMap<String, String> newAttributes) throws Exception {
        JSONObject payloadObject;

        // TODO need to set in newAttributes:
        //          event_id                            DONE
        //          operation (INSERT, REMOVE, MODIFY)  DONE
        //          extract_method                      DONE
        //          event_timestamp                     DONE

        if (dynamodbStreamObject.isNull(oldImage)) {

            newAttributes.put(MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(), MetadataFields.ExtractMethod.SNAPSHOT.getValue());
            newAttributes.put(MetadataFields.Operation.OPERATION.getValue(), MetadataFields.Operation.INSERT.getValue());

        }
        if (dynamodbStreamObject.getJSONObject(oldImage).isEmpty() && dynamodbStreamObject.has(newImage)) {

            newAttributes.put(MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(), MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.Operation.OPERATION.getValue(), MetadataFields.Operation.INSERT.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(newImage);

        } else if ((dynamodbStreamObject.isNull(newImage) || dynamodbStreamObject.getJSONObject(newImage).isEmpty()) && dynamodbStreamObject.has(oldImage)) {

            newAttributes.put(MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(), MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.Operation.OPERATION.getValue(), MetadataFields.Operation.REMOVE.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(oldImage);

        } else if (dynamodbStreamObject.has(newImage) && dynamodbStreamObject.has(oldImage)) {

            newAttributes.put(MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(), MetadataFields.ExtractMethod.CDC.getValue());
            newAttributes.put(MetadataFields.Operation.OPERATION.getValue(), MetadataFields.Operation.MODIFY.getValue());
            payloadObject = dynamodbStreamObject.getJSONObject(newImage);

        } else {
            throw new CustomExceptionsUtils.MalformedEventException(
                    "No NewImage or OldImage found in message. Maybe the provider is not configured"
                            + " correctly?");
        }

        // add event_time to payload root for streaming analytics use cases
        if (dynamodbStreamObject.isNull(MetadataFields.Event.EVENT_TIMESTAMP.getValue())) {
            if (!dynamodbStreamObject.isNull(published)) {
                payloadObject.put(MetadataFields.Event.EVENT_TIMESTAMP.getValue(), dynamodbStreamObject.getString(published));
                // Used for backfill purposes
            } else if (attributes.containsKey(timestamp)) {
                payloadObject.put(MetadataFields.Event.EVENT_TIMESTAMP.getValue(), attributes.get(timestamp));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from dynamoDB stream event as attributes
        if (!dynamodbStreamObject.isNull(eventId)) {
            newAttributes.put(MetadataFields.Event.EVENT_ID.getValue(), dynamodbStreamObject.getString(eventId));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `EventId` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), newAttributes);
    }
}
