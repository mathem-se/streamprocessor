package org.streamprocessor.core.transforms;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.caches.DataContractCache;
import org.streamprocessor.core.helpers.CustomEventHelper;
import org.streamprocessor.core.helpers.DynamodbHelper;
import org.streamprocessor.core.helpers.SalesforceHelper;

public class TransformMessageFn extends DoFn<PubsubMessage, PubsubMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(TransformMessageFn.class);
    static final long serialVersionUID = 238L;

    String dataContractsServiceUrl;

    public TransformMessageFn(String dataContractsServiceUrl) {
        this.dataContractsServiceUrl = dataContractsServiceUrl;
    }

    private class UnknownPorviderException extends Exception {
        private UnknownPorviderException(String errorMessage) {
            super(errorMessage);
        }
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage received, OutputReceiver<PubsubMessage> out) {
        String uuid = null;
        String entity = null;
        PubsubMessage msg;

        try {
            String receivedPayload = new String(received.getPayload(), StandardCharsets.UTF_8);
            JSONObject streamObject = new JSONObject(receivedPayload);

            HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.putAll(received.getAttributeMap());
            attributes.put(
                    "processing_timestamp",
                    Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

            // use topic attribute as entity attribute if entity is missing
            if (received.getAttribute("entity") == null && received.getAttribute("topic") != null) {
                attributes.put("entity", received.getAttribute("topic"));
            }

            uuid = received.getAttribute("uuid");
            entity = attributes.get("entity");
            String endpoint =
                    dataContractsServiceUrl.replaceAll("/$", "") + "/" + "contract/" + entity;

            JSONObject dataContract = DataContractCache.getDataContractFromCache().get(endpoint);
            String provider =
                    dataContract
                            .getJSONObject("endpoint")
                            .getJSONObject("source")
                            .getString("provider");

            switch (provider) {
                case "dynamodb":
                    msg = DynamodbHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "salesforce":
                    msg = SalesforceHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "custom_event":
                    msg = CustomEventHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                default:
                    throw new UnknownPorviderException(
                            "Provider: "
                                    + provider
                                    + ".\n"
                                    + "Either the provider is not supported or the data contract is"
                                    + " not valid.");
            }

            out.output(msg);

        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}] entity[{}] uuid[{}]",
                    e.getClass().getName(),
                    "TransformMessageFn.processElement()",
                    e.toString(),
                    entity,
                    uuid);
        }
    }
}
