/*
 * Copyright (C) 2021 Robert Sahlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.streamprocessor.core.transforms;

import com.google.api.services.bigquery.model.TableRow;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.caches.DataContractsCache;
import org.streamprocessor.core.caches.SchemaCache;
import org.streamprocessor.core.helpers.FailsafeElement;
import org.streamprocessor.core.utils.BqUtils;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class SerializeMessageToRowFn
        extends DoFn<FailsafeElement<PubsubMessage>, FailsafeElement<Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(SerializeMessageToRowFn.class);

    TupleTag<FailsafeElement<Row>> successTag;
    TupleTag<FailsafeElement<Row>> failureTag;
    String unknownFieldLogger;
    String format;
    String projectId;
    String dataContractsServiceUrl;
    float ratio;

    public SerializeMessageToRowFn(
            TupleTag<FailsafeElement<Row>> successTag,
            TupleTag<FailsafeElement<Row>> failureTag,
            String projectId,
            String dataContractsServiceUrl,
            float ratio) {
        this.successTag = successTag;
        this.failureTag = failureTag;
        this.projectId = projectId;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.ratio = ratio;
    }

    public SerializeMessageToRowFn(
            TupleTag<FailsafeElement<Row>> successTag,
            TupleTag<FailsafeElement<Row>> failureTag,
            String projectId,
            String dataContractsServiceUrl) {
        this.successTag = successTag;
        this.failureTag = failureTag;
        this.projectId = projectId;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.ratio = 0.001f;
    }

    @ProcessElement
    public void processElement(
            @Element FailsafeElement<PubsubMessage> received, MultiOutputReceiver out)
            throws Exception {

        PubsubMessage pubsubMessage = received.getCurrentElement();
        FailsafeElement<Row> outputElement;
        Row row = null;

        String entity = pubsubMessage.getAttribute("entity").replace("-", "_").toLowerCase();
        String payload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
        String endpoint = dataContractsServiceUrl.replaceAll("/$", "") + "/" + "contract/" + entity;

        Map<String, String> attributesMap = pubsubMessage.getAttributeMap();

        try {
            JSONObject dataContract = DataContractsCache.getDataContractFromCache(endpoint);
            String datasetId =
                    dataContract
                            .getJSONObject("endpoints")
                            .getJSONObject("target")
                            .getString("dataset");

            String linkedResource =
                    String.format(
                            "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                            projectId, datasetId, entity);

            Schema schema = SchemaCache.getSchemaFromCache(linkedResource);
            if (schema.getFieldCount() == 0) {
                throw new CustomExceptionsUtils.NoSchemaException(
                        "Schema for " + linkedResource + " doesn't exist");
            }

            JSONObject payloadJson = new JSONObject(payload);

            /* TODO: is this needed?
            if (payloadJson.isNull("event_timestamp")) {
                payloadJson.put("event_timestamp", DateTime.now().withZone(DateTimeZone.UTC).toString());
            }
            */

            JSONObject attributesJson = new JSONObject(pubsubMessage.getAttributeMap());
            payloadJson.put("_metadata", attributesJson);

            TableRow tr = BqUtils.convertJsonToTableRow(payloadJson.toString());

            row = BqUtils.toBeamRow(schema, tr);

            outputElement = new FailsafeElement<>(received.getOriginalElement(), row);

            out.get(successTag).output(outputElement);

        } catch (Exception e) {
            outputElement =
                    new FailsafeElement<>(
                            received.getOriginalElement(),
                            row,
                            "TransformMessageFn.processElement()",
                            e.getClass().getName(),
                            e);

            LOG.error(
                    "exception[{}] step[{}] details[{}] entity[{}]",
                    outputElement.getException(),
                    outputElement.getPipelineStep(),
                    outputElement.getExceptionDetails(),
                    entity);

            out.get(failureTag).output(outputElement);
        }
    }
}
