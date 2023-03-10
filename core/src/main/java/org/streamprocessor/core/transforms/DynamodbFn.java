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

public class DynamodbFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamodbFn.class);
    static final long serialVersionUID = 238L;

    String unknownFieldLogger;
    String format;

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    public DynamodbFn() {}

    @ProcessElement
    public void processElement(@Element PubsubMessage received, OutputReceiver<PubsubMessage> out)
            throws Exception {
        try {
            String receivedPayload = new String(received.getPayload(), StandardCharsets.UTF_8);
            JSONObject dynamodbStreamObject = new JSONObject(receivedPayload);

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
                // add operation and payload according to dynamodb 'NEW_AND_OLD_IMAGES' stream view
                // type
                if ((dynamodbStreamObject.isNull("OldImage")
                                || dynamodbStreamObject.getJSONObject("OldImage").isEmpty())
                        && dynamodbStreamObject.has("NewImage")) {
                    attributes.put("operation", "INSERT");
                    payloadObject = dynamodbStreamObject.getJSONObject("NewImage");
                } else if ((dynamodbStreamObject.isNull("NewImage")
                                || dynamodbStreamObject.getJSONObject("NewImage").isEmpty())
                        && dynamodbStreamObject.has("OldImage")) {
                    attributes.put("operation", "REMOVE");
                    payloadObject = dynamodbStreamObject.getJSONObject("OldImage");
                } else if (dynamodbStreamObject.has("NewImage")
                        && dynamodbStreamObject.has("OldImage")) {
                    attributes.put("operation", "MODIFY");
                    payloadObject = dynamodbStreamObject.getJSONObject("NewImage");
                } else {
                    // Not a dynamoDB change event
                    payloadObject = dynamodbStreamObject;
                }

                // Add meta-data from dynamoDB stream event as attributes
                if (!dynamodbStreamObject.isNull("Published")) {
                    attributes.put(
                            "dynamodbPublished", dynamodbStreamObject.getString("Published"));
                }

                // add event_time to payload root for streaming analytics use cases
                if (dynamodbStreamObject.isNull("event_timestamp")) {
                    if (!dynamodbStreamObject.isNull("Published")) {
                        payloadObject.put(
                                "event_timestamp", dynamodbStreamObject.getString("Published"));
                    } else {
                        LOG.error("No event_timestamp found in event");
                    }
                }

                // Add meta-data from dynamoDB stream event as attributes
                if (!dynamodbStreamObject.isNull("EventId")) {
                    attributes.put("dynamodbEventId", dynamodbStreamObject.getString("EventId"));
                // Add meta-data from custom events as attributes
                } else if (!dynamodbStreamObject.isNull("event_id")) {
                    attributes.put("event_id", payloadObject.getString("event_id"));
                } else {
                    LOG.error("No event_id found in event");
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
