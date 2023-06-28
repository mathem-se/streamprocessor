package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CustomExceptionsUtils;
import org.streamprocessor.core.transforms.MetadataFields;

public class SalesforceHelper {

    public static final String detail = "detail";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject salesforceStreamObject, HashMap<String, String> attributes, HashMap<String, String> newAttributes)
            throws Exception {
        JSONObject payloadObject;

        // TODO need to set in newAttributes:
        //          event_id
        //          operation (INSERT, REMOVE, MODIFY)
        //          extract_method
        //          event_timestamp

        // salesforce events passed through Appflow
        // have their payload nested within the `detail` field
        // other fields just contain metadata from Appflow
        if (salesforceStreamObject.has("detail")
                && attributes.get("entity").toLowerCase().startsWith("salesforce")) {
            payloadObject = salesforceStreamObject.getJSONObject("detail");
        } else {
            // Not a salesforce detail event
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `detail` element found in message. Not a Saleseforce event?");
        }

        // Add meta-data from salesforce stream event as attributes
        // TODO: consolidate to be consistent with dynamodb events
        if (!salesforceStreamObject.isNull("time")) {
            attributes.put("salesforcePublished", salesforceStreamObject.getString("time"));
            payloadObject.put(MetadataFields.Event.EVENT_TIMESTAMP.getValue(), salesforceStreamObject.getString("time"));
        } else if (attributes.containsKey("timestamp") && !attributes.get("timestamp").isEmpty()) {
            attributes.put("salesforcePublished", attributes.get("timestamp"));
            payloadObject.put(MetadataFields.Event.EVENT_TIMESTAMP.getValue(), attributes.get("timestamp"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `time` or `timestamp` found in message");
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull("id")) {
            attributes.put(MetadataFields.Event.EVENT_ID.getValue(), salesforceStreamObject.getString("id"));
        } else if (!attributes.containsKey(MetadataFields.Event.EVENT_ID.getValue())) {
            attributes.put(MetadataFields.Event.EVENT_ID.getValue(), attributes.get("uuid"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
