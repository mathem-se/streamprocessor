package org.streamprocessor.core.helpers;

import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class SalesforceHelper {
    public static class MissingMetadataException extends Exception {
        private MissingMetadataException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static PubsubMessage enrichPubsubMessage(
            JSONObject salesforceStreamObject, HashMap<String, String> attributes)
            throws Exception {
        JSONObject payloadObject;

        // salesforce events passed through Appflow
        // have their payload nested within the `detail` field
        // other fields just contain metadata from Appflow
        // TODO: abstract this away to another service that tells you which fields to look
        // at
        if (salesforceStreamObject.has("detail")
                && attributes.get("topic").toLowerCase().startsWith("salesforce")) {
            payloadObject = salesforceStreamObject.getJSONObject("detail");
        } else {
            // Not a salesforce detail event
            payloadObject = salesforceStreamObject;
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull("time")) {
            attributes.put("salesforcePublished", salesforceStreamObject.getString("time"));
        }

        // add event_time to payload root for streaming analytics use cases
        // TODO: consolidate to be consistent with dynamodb events
        if (salesforceStreamObject.isNull("event_timestamp")) {
            if (!salesforceStreamObject.isNull("time")) {
                payloadObject.put("event_timestamp", salesforceStreamObject.getString("time"));
            } else if (attributes.containsKey("timestamp")
                    && !attributes.get("timestamp").isEmpty()) {
                payloadObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new MissingMetadataException("No event_timestamp found in message");
            }
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull("id")) {
            attributes.put("event_id", salesforceStreamObject.getString("id"));
        } else if (!attributes.containsKey("event_id")) {
            attributes.put("event_id", attributes.get("uuid"));
        }

        return new PubsubMessage(payloadObject.toString().getBytes("UTF-8"), attributes);
    }
}
