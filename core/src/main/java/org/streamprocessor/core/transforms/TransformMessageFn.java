package org.streamprocessor.core.transforms;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.caches.DataContractsCache;
import org.streamprocessor.core.helpers.CustomEventHelper;
import org.streamprocessor.core.helpers.DynamodbHelper;
import org.streamprocessor.core.helpers.SalesforceHelper;
import org.streamprocessor.core.utils.CustomExceptionsUtils;
import org.streamprocessor.core.values.FailsafeElement;

public class TransformMessageFn
        extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, PubsubMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(TransformMessageFn.class);

    private static final Counter failureCounter = Metrics.counter("TransformMessageFn", "failures");
    static final long serialVersionUID = 238L;

    String name;
    String version;
    String dataContractsServiceUrl;
    TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> successTag;
    TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> failureTag;

    public TransformMessageFn(
            String name,
            String version,
            String dataContractsServiceUrl,
            TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> successTag,
            TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> failureTag) {
        this.name = name;
        this.version = version;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage received, MultiOutputReceiver out) {
        String uuid = null;
        String entity = null;
        PubsubMessage currentElement = null;
        FailsafeElement<PubsubMessage, PubsubMessage> outputElement;

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

            JSONObject dataContract = DataContractsCache.getDataContractFromCache(endpoint);
            String provider =
                    dataContract
                            .getJSONObject("endpoints")
                            .getJSONObject("source")
                            .getString("provider");

            LocalDate currentDate = LocalDate.now(ZoneId.of("UTC"));

            HashMap<String, String> newAttributes = new HashMap<String, String>();
            if (received.getAttribute("gcp-backfill") != null) {
                newAttributes.put("gcp-backfill", received.getAttribute("gcp-backfill"));
            }
            ArrayList<String> traceList = new ArrayList<String>();
            for (Map.Entry<String, String> set : received.getAttributeMap().entrySet()) {
                String setKey = set.getKey();
                String setValue = set.getValue();

                if (setKey.startsWith("trace_")) {
                    JSONObject traceObject = new JSONObject(setValue);
                    HashMap<String, String> traceMap = new HashMap<String, String>();
                    if (traceObject.has("timestamp")) {
                        traceMap.put("timestamp", traceObject.getString("timestamp"));
                    }
                    if (traceObject.has("id")) {
                        traceMap.put("id", traceObject.getString("id"));
                    }
                    if (traceObject.has("service")) {
                        traceMap.put("service", traceObject.getString("service"));
                    }
                    if (traceObject.has("version")) {
                        traceMap.put("version", traceObject.getString("version"));
                    }
                    traceList.add(new JSONObject(traceMap).toString());
                }
            }

            HashMap<String, String> traceMap = new HashMap<String, String>();
            traceMap.put("timestamp", Instant.now().toString());
            traceMap.put("id", UUID.randomUUID().toString());
            traceMap.put("service", name);
            traceMap.put("version", version);
            traceList.add(new JSONObject(traceMap).toString());

            newAttributes.put("entity", dataContract.getString("entity"));
            newAttributes.put("data_contracts_schema_version", dataContract.getString("version"));
            newAttributes.put("provider", provider);
            if (traceList.size() > 0) {
                newAttributes.put("trace", traceList.toString());
            }

            String backfill = received.getAttribute("backfill");
            if (backfill != null) {
                newAttributes.put("backfill", received.getAttribute("backfill"));
            }

            if (dataContract.isNull("valid_from")) {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `valid_from` found in data contract");
            } else {
                String validFrom = dataContract.getString("valid_from");
                LocalDate validFromDate = LocalDate.parse(validFrom);

                if (currentDate.isBefore(validFromDate)) {
                    throw new CustomExceptionsUtils.InactiveDataContractException(
                            "Data contract is not valid for the current time. "
                                    + "Data contract is valid from: "
                                    + validFrom);
                } else if (!dataContract.isNull("valid_to")) {
                    String validTo = dataContract.getString("valid_to");
                    LocalDate validToDate = LocalDate.parse(validTo);

                    if (currentDate.isAfter(validToDate)) {
                        throw new CustomExceptionsUtils.InactiveDataContractException(
                                "Data contract is not valid for the current time. "
                                        + "Data contract is valid from: "
                                        + validFrom
                                        + " to: "
                                        + validTo);
                    }
                }
            }

            switch (provider) {
                case "dynamodb":
                    currentElement =
                            DynamodbHelper.enrichPubsubMessage(
                                    streamObject, attributes, newAttributes);
                    break;
                case "salesforce":
                    currentElement =
                            SalesforceHelper.enrichPubsubMessage(
                                    streamObject, attributes, newAttributes);
                    break;
                case "custom_event":
                case "marketing_cloud":
                case "pi":
                    currentElement =
                            CustomEventHelper.enrichPubsubMessage(
                                    streamObject, attributes, newAttributes);
                    break;
                default:
                    throw new CustomExceptionsUtils.UnknownPorviderException(
                            "Provider: "
                                    + provider
                                    + ".\n"
                                    + "Either the provider is not supported or the data contract is"
                                    + " not valid.");
            }

            outputElement = new FailsafeElement<>(received, currentElement);

            out.get(successTag).output(outputElement);
        } catch (Exception e) {
            failureCounter.inc();

            outputElement =
                    new FailsafeElement<>(received, currentElement)
                            .setPipelineStep("TransformMessageFn.processElement()")
                            .setExceptionType(e.getClass().getName())
                            .setExceptionDetails(e.toString())
                            .setEventTimestamp(Instant.now().toString());

            LOG.error(
                    "exception[{}] step[{}] details[{}] entity[{}] uuid[{}]",
                    outputElement.getExceptionType(),
                    outputElement.getPipelineStep(),
                    outputElement.getExceptionDetails(),
                    entity,
                    uuid);

            out.get(failureTag).output(outputElement);
        }
    }
}
