package org.streamprocessor.core.transforms;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.caches.DataContractsCache;
import org.streamprocessor.core.helpers.CloudStorageImportHelper;
import org.streamprocessor.core.helpers.CustomEventHelper;
import org.streamprocessor.core.helpers.DynamodbHelper;
import org.streamprocessor.core.helpers.GoogleSheetsImportHelper;
import org.streamprocessor.core.helpers.SalesforceHelper;
import org.streamprocessor.core.helpers.MySQLDataStreamHelper;
import org.streamprocessor.core.utils.BqUtils;
import org.streamprocessor.core.utils.CustomExceptionsUtils;
import org.streamprocessor.core.values.FailsafeElement;

public class TransformMessageFn
        extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, PubsubMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(TransformMessageFn.class);
    private static final Counter failureCounter = Metrics.counter("TransformMessageFn", "failures");
    static final long serialVersionUID = 238L;

    private static final String METADATA = "_metadata";

    String jobName;
    String version;
    String dataContractsServiceUrl;
    TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> successTag;
    TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> failureTag;

    public TransformMessageFn(
            String jobName,
            String version,
            String dataContractsServiceUrl,
            TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> successTag,
            TupleTag<FailsafeElement<PubsubMessage, PubsubMessage>> failureTag) {
        this.jobName = jobName;
        this.version = version;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage received, MultiOutputReceiver out) {
        String uuid = null;
        String entity = null;
        JSONObject transformedPayload = null;
        PubsubMessage currentElement = null;
        FailsafeElement<PubsubMessage, PubsubMessage> outputElement;

        try {
            String receivedPayload = new String(received.getPayload(), StandardCharsets.UTF_8);
            JSONObject streamObject = new JSONObject(receivedPayload);

            HashMap<String, String> attributes = new HashMap<String, String>();
            HashMap<String, String> metadata = new HashMap<String, String>();
            attributes.putAll(received.getAttributeMap());

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

            if (received.getAttribute("gcp_backfill") != null) {
                metadata.put("gcp_backfill", received.getAttribute("gcp_backfill"));
            }
            ArrayList<JSONObject> traceList = new ArrayList<JSONObject>();
            for (Map.Entry<String, String> set : received.getAttributeMap().entrySet()) {
                String setKey = set.getKey();
                String setValue = set.getValue();

                if (setKey.startsWith("trace_")) {
                    JSONObject traceObject = new JSONObject(setValue);
                    traceList.add(traceObject);
                }
            }

            HashMap<String, String> traceMap = new HashMap<String, String>();
            traceMap.put("timestamp", Instant.now().toString());
            traceMap.put("id", UUID.randomUUID().toString());
            traceMap.put("service", "streamprocessor-" + jobName);
            traceMap.put("version", version);
            traceList.add(new JSONObject(traceMap));

            metadata.put("entity", dataContract.getString("entity"));
            metadata.put("data_contracts_schema_version", dataContract.getString("version"));
            metadata.put("provider", provider);

            String backfill = received.getAttribute("backfill");
            if (backfill != null) {
                metadata.put("backfill", received.getAttribute("backfill"));
            }

            JSONObject metadataJson = new JSONObject(metadata);
            metadataJson.put("trace", new JSONArray(traceList));
            streamObject.put(METADATA, metadataJson);

            switch (provider) {
                case "dynamodb":
                    transformedPayload =
                            DynamodbHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "salesforce":
                    transformedPayload =
                            SalesforceHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "custom_event":
                case "marketing_cloud":
                case "pi":
                    transformedPayload =
                            CustomEventHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "cloud_storage_import":
                    transformedPayload =
                            CloudStorageImportHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "google_sheets_import":
                    transformedPayload =
                            GoogleSheetsImportHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                case "mysql_datastream":
                    transformedPayload =
                            MySQLDataStreamHelper.enrichPubsubMessage(streamObject, attributes);
                    break;
                default:
                    throw new CustomExceptionsUtils.UnknownProviderException(
                            String.format(
                                    "Provider %s. Either the provider is not supported or the data"
                                            + " contract is not valid.",
                                    provider));
            }
            String eventTimestamp =
                    transformedPayload.get(MetadataFields.EVENT_TIMESTAMP).toString();
            DateTime eventDateTime = BqUtils.convertStringToDatetime(entity, eventTimestamp);
            if (dataContract.isNull("valid_from")) {
                throw new CustomExceptionsUtils.MissingMetadataException(
                        "No `valid_from` found in data contract");
            } else {
                String validFrom = dataContract.getString("valid_from");
                LocalDate validFromDate = LocalDate.parse(validFrom);

                if (eventDateTime.toLocalDate().isBefore(validFromDate)) {
                    throw new CustomExceptionsUtils.InactiveDataContractException(
                            String.format(
                                    "Data contract is not valid for the current time. Data contract"
                                        + " is valid from: %s eventDate is %s, eventTimestamp is"
                                        + " %s",
                                    validFrom, eventDateTime, eventTimestamp));

                } else if (!dataContract.isNull("valid_to")) {
                    String validTo = dataContract.getString("valid_to");
                    LocalDate validToDate = LocalDate.parse(validTo);

                    if (eventDateTime.toLocalDate().isAfter(validToDate)) {
                        throw new CustomExceptionsUtils.InactiveDataContractException(
                                String.format(
                                        "Data contract is not valid for the current time. Data"
                                            + " contract is valid from: %s to %s eventTimestamp is"
                                            + " %s.",
                                        validFrom, validTo, eventTimestamp));
                    }
                }
            }

            currentElement =
                    new PubsubMessage(
                            transformedPayload.toString().getBytes(StandardCharsets.UTF_8),
                            attributes);

            outputElement = new FailsafeElement<>(received, currentElement);

            out.get(successTag).output(outputElement);
        } catch (Exception e) {
            failureCounter.inc();

            outputElement =
                    new FailsafeElement<>(received, currentElement)
                            .setJobName(jobName)
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
