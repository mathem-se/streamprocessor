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
import java.time.Instant;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.caches.DataContractsCache;
import org.streamprocessor.core.caches.SchemaCache;
import org.streamprocessor.core.utils.BqUtils;
import org.streamprocessor.core.utils.CustomExceptionsUtils;
import org.streamprocessor.core.values.FailsafeElement;

public class SerializeMessageToRowFn
        extends DoFn<
                FailsafeElement<PubsubMessage, PubsubMessage>,
                FailsafeElement<PubsubMessage, Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(SerializeMessageToRowFn.class);

    private static final Counter failureCounter =
            Metrics.counter("SerializeMessageToRowFn", "failures");

    TupleTag<FailsafeElement<PubsubMessage, Row>> successTag;
    TupleTag<FailsafeElement<PubsubMessage, Row>> failureTag;
    String jobName;
    String projectId;
    String dataContractsServiceUrl;
    float ratio;

    public SerializeMessageToRowFn(
            TupleTag<FailsafeElement<PubsubMessage, Row>> successTag,
            TupleTag<FailsafeElement<PubsubMessage, Row>> failureTag,
            String jobName,
            String projectId,
            String dataContractsServiceUrl,
            float ratio) {
        this.successTag = successTag;
        this.failureTag = failureTag;
        this.jobName = jobName;
        this.projectId = projectId;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.ratio = ratio;
    }

    public SerializeMessageToRowFn(
            TupleTag<FailsafeElement<PubsubMessage, Row>> successTag,
            TupleTag<FailsafeElement<PubsubMessage, Row>> failureTag,
            String jobName,
            String projectId,
            String dataContractsServiceUrl,
            Boolean relaxedStrictness) {
        this.successTag = successTag;
        this.failureTag = failureTag;
        this.jobName = jobName;
        this.projectId = projectId;
        this.dataContractsServiceUrl = dataContractsServiceUrl;
        this.ratio = 0.001f;
    }

    @ProcessElement
    public void processElement(
            @Element FailsafeElement<PubsubMessage, PubsubMessage> received,
            MultiOutputReceiver out)
            throws Exception {

        PubsubMessage pubsubMessage = received.getCurrentElement();
        FailsafeElement<PubsubMessage, Row> outputElement;
        Row currentElement = null;

        String entity = pubsubMessage.getAttribute("entity").replace("-", "_").toLowerCase();
        String uuid = pubsubMessage.getAttribute("uuid");
        String payload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
        String endpoint = dataContractsServiceUrl.replaceAll("/$", "") + "/" + "contract/" + entity;

        try {
            JSONObject dataContract = DataContractsCache.getDataContractFromCache(endpoint);
            String datasetId =
                    dataContract
                            .getJSONObject("endpoints")
                            .getJSONObject("source")
                            .getString("provider");

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

            TableRow tr = BqUtils.convertJsonToTableRow(entity, payloadJson.toString());

            currentElement = BqUtils.toBeamRow(entity, schema, tr, true);

            outputElement = new FailsafeElement<>(received.getOriginalElement(), currentElement);

            out.get(successTag).output(outputElement);

        } catch (Exception e) {
            failureCounter.inc();

            outputElement =
                    new FailsafeElement<>(received.getOriginalElement(), currentElement)
                            .setJobName(jobName)
                            .setPipelineStep("SerializeMessageToRowFn.processElement()")
                            .setExceptionType(e.getClass().getName())
                            .setExceptionDetails(e.toString())
                            .setEventTimestamp(Instant.now().toString());

            LOG.error(
                    "exception[{}] step[{}] details[{}] entity[{}] uud[{}]",
                    outputElement.getExceptionType(),
                    outputElement.getPipelineStep(),
                    outputElement.getExceptionDetails(),
                    entity,
                    uuid);

            out.get(failureTag).output(outputElement);
        }
    }
}
