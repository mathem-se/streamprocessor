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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesforceFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceFn.class);
    static final long serialVersionUID = 238L;

    String unknownFieldLogger;
    String format;

    private class MissingMetadataException extends Exception {

        private MissingMetadataException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    public SalesforceFn() {}

    @ProcessElement
    public void processElement(@Element PubsubMessage received, OutputReceiver<PubsubMessage> out)
            throws Exception {
        try {
            String receivedPayload = new String(received.getPayload(), StandardCharsets.UTF_8);
            JSONObject salesforceStreamObject = new JSONObject(receivedPayload);

            HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.putAll(received.getAttributeMap());
            attributes.put(
                    "processing_timestamp",
                    Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

            // use topic attribute as entity attribute if entity is missing
            if (received.getAttribute("entity") == null && received.getAttribute("topic") != null) {
                attributes.put("entity", received.getAttribute("topic"));
            }

            String entity = attributes.get("entity");
            JSONObject payloadObject;

            try {
                // salesforce events passed through Appflow
                // have their payload nested within the `detail` field
                // other fields just contain metadata from Appflow
                // TODO: abstract this away to another service that tells you which fields to look
                // at
                if (salesforceStreamObject.has("detail")
                        && received.getAttribute("topic").toLowerCase().startsWith("salesforce")) {
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
                        payloadObject.put(
                                "event_timestamp", salesforceStreamObject.getString("time"));
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

                out.output(
                        new PubsubMessage(payloadObject.toString().getBytes("UTF-8"), attributes));
            } catch (IllegalArgumentException e) {
                LOG.error(
                        "exception[{}] step[{}] details[{}] entity[{}]",
                        e.getClass().getName(),
                        "DynamodbFn.processElement()",
                        e.toString(),
                        entity);
            } catch (org.json.JSONException e) {
                LOG.error(
                        "exception[{}] step[{}] details[{}] entity[{}]",
                        e.getClass().getName(),
                        "DynamodbFn.processElement()",
                        e.toString(),
                        entity);
            }
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "DynamodbFn.processElement()",
                    e.toString());
        }
    }
}
