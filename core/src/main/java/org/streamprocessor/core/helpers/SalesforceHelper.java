package org.streamprocessor.core.helpers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class SalesforceHelper {
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
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `detail` element found in message. Not a Saleseforce event?");
        }

        // Add meta-data from salesforce stream event as attributes
        // TODO: Throw exception if these fields are missing, align with data producers first.
        if (!salesforceStreamObject.isNull("time")) {
            attributes.put("salesforcePublished", salesforceStreamObject.getString("time"));
            // Used for backfill purposes
        } else if (attributes.containsKey("timestamp")) {
            attributes.put("salesforcePublished", attributes.get("timestamp"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException("No `time` found in message");
        }

        // TODO: consolidate to be consistent with dynamodb events

        // add event_time to payload root for streaming analytics use cases
        if (salesforceStreamObject.isNull("event_timestamp")) {
            if (!salesforceStreamObject.isNull("time")) {
                payloadObject.put("event_timestamp", salesforceStreamObject.getString("time"));
                // Used for backfill purposes
            } else if (attributes.containsKey("timestamp")
                    && !attributes.get("timestamp").isEmpty()) {
                payloadObject.put("event_timestamp", attributes.get("timestamp"));
            } else {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `event_timestamp` found in message");
            }
        }

        // Add meta-data from salesforce stream event as attributes
        if (!salesforceStreamObject.isNull("id")) {
            attributes.put("event_id", salesforceStreamObject.getString("id"));
        } else if (!attributes.containsKey("event_id")) {
            attributes.put("event_id", attributes.get("uuid"));
        } else {
            throw new CustomExceptionsUtils.MissingMetadataException(
                    "No `id` or `uuid` found in message.");
        }

        return new PubsubMessage(
                payloadObject.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
