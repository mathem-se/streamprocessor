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
                MetadataFields.EXTRACT_METHOD, MetadataFields.ExtractMethod.CDC.getValue());

        String changeType = payloadObject.getString(CHANGE_TYPE);
        if (changeType.equals(CREATE)) {
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.INSERT.getValue());
        } else if (changeType.equals(UPDATE)) {
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.MODIFY.getValue());
        } else if (changeType.equals(DELETE)) {
            newAttributes.put(MetadataFields.OPERATION, MetadataFields.Operation.REMOVE.getValue());
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `ChangeType__c` found in message");
        }

        if (!salesforceStreamObject.isNull(TIME)) {
            payloadObject.put(
                    MetadataFields.EVENT_TIMESTAMP, salesforceStreamObject.getString(TIME));
        } else if (attributes.containsKey(TIMESTAMP) && !attributes.get(TIMESTAMP).isEmpty()) {
            payloadObject.put(MetadataFields.EVENT_TIMESTAMP, attributes.get(TIMESTAMP));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `time` or `timestamp` found in message");
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull(ID)) {
            newAttributes.put(MetadataFields.EVENT_ID, salesforceStreamObject.getString(ID));
        } else if (!attributes.containsKey(MetadataFields.EVENT_ID)) {
            newAttributes.put(MetadataFields.EVENT_ID, attributes.get(UUID));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), newAttributes);
    }
}
