package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class SalesforceHelper {

    public static final String DETAIL = "detail";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject salesforceStreamObject,
            HashMap<String, String> attributes,
            HashMap<String, String> newAttributes)
            throws Exception {
        JSONObject payloadObject;

        // TODO need to set in newAttributes:
        //          event_id                            DONE
        //          operation (INSERT, REMOVE, MODIFY)  DONE
        //          extract_method
        //          event_timestamp                     DONE

        // salesforce events passed through Appflow
        // have their payload nested within the `detail` field
        // other fields just contain metadata from Appflow
        if (salesforceStreamObject.has(DETAIL)
                && attributes.get("entity").toLowerCase().startsWith("salesforce")) {
            payloadObject = salesforceStreamObject.getJSONObject(DETAIL);
        } else {
            // Not a salesforce detail event
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `detail` element found in message. Not a Saleseforce event?");
        }

        String changeType = payloadObject.getString("ChangeType__c");
        if (changeType == "CREATE") {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.INSERT.getValue());
        } else if (changeType == "UPDATE") {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.MODIFY.getValue());
        } else if (changeType == "DELETE") {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.REMOVE.getValue());
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `ChangeType__c` found in message");
        }

        if (!salesforceStreamObject.isNull("time")) {
            payloadObject.put(
                    MetadataFields.Event.EVENT_TIMESTAMP.getValue(),
                    salesforceStreamObject.getString("time"));
        } else if (attributes.containsKey("timestamp") && !attributes.get("timestamp").isEmpty()) {
            payloadObject.put(
                    MetadataFields.Event.EVENT_TIMESTAMP.getValue(), attributes.get("timestamp"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `time` or `timestamp` found in message");
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull("id")) {
            newAttributes.put(
                    MetadataFields.Event.EVENT_ID.getValue(),
                    salesforceStreamObject.getString("id"));
        } else if (!attributes.containsKey(MetadataFields.Event.EVENT_ID.getValue())) {
            newAttributes.put(MetadataFields.Event.EVENT_ID.getValue(), attributes.get("uuid"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
