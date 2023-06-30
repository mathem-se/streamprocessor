package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.transforms.MetadataFields;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class SalesforceHelper {

    public static final String DETAIL = "detail";
    public static final String ENTITY = "entity";
    public static final String SALESFORCE = "salesforce";
    public static final String CHANGE_TYPE = "ChangeType__c";
    public static final String CREATE = "CREATE";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String TIME = "time";
    public static final String TIMESTAMP = "timestamp";
    public static final String ID = "id";
    public static final String UUID = "uuid";

    public static PubsubMessage enrichPubsubMessage(
            JSONObject salesforceStreamObject,
            HashMap<String, String> attributes,
            HashMap<String, String> newAttributes)
            throws Exception {
        JSONObject payloadObject;

        // TODO need to set in newAttributes:
        //          event_id                            DONE
        //          operation (INSERT, REMOVE, MODIFY)  DONE
        //          extract_method                      DONE
        //          event_timestamp                     DONE

        // salesforce events passed through Appflow
        // have their payload nested within the `detail` field
        // other fields just contain metadata from Appflow
        if (salesforceStreamObject.has(DETAIL)
                && attributes.get(ENTITY).toLowerCase().startsWith(SALESFORCE)) {
            payloadObject = salesforceStreamObject.getJSONObject(DETAIL);
        } else {
            // Not a salesforce detail event
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `detail` element found in message. Not a Saleseforce event?");
        }

        newAttributes.put(
                MetadataFields.ExtractMethod.EXTRACT_METHOD.getValue(),
                MetadataFields.ExtractMethod.CDC.getValue());

        String changeType = payloadObject.getString(CHANGE_TYPE);
        if (changeType == CREATE) {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.INSERT.getValue());
        } else if (changeType == UPDATE) {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.MODIFY.getValue());
        } else if (changeType == DELETE) {
            newAttributes.put(
                    MetadataFields.Operation.OPERATION.getValue(),
                    MetadataFields.Operation.REMOVE.getValue());
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `ChangeType__c` found in message");
        }

        if (!salesforceStreamObject.isNull(TIME)) {
            payloadObject.put(
                    MetadataFields.Event.EVENT_TIMESTAMP.getValue(),
                    salesforceStreamObject.getString(TIME));
        } else if (attributes.containsKey(TIMESTAMP) && !attributes.get(TIMESTAMP).isEmpty()) {
            payloadObject.put(
                    MetadataFields.Event.EVENT_TIMESTAMP.getValue(), attributes.get(TIMESTAMP));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `time` or `timestamp` found in message");
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull(ID)) {
            newAttributes.put(
                    MetadataFields.Event.EVENT_ID.getValue(), salesforceStreamObject.getString(ID));
        } else if (!attributes.containsKey(MetadataFields.Event.EVENT_ID.getValue())) {
            newAttributes.put(MetadataFields.Event.EVENT_ID.getValue(), attributes.get(UUID));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), newAttributes);
    }
}
