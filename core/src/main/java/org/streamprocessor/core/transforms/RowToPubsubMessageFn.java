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
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.values.FailsafeElement;

public class RowToPubsubMessageFn
        extends DoFn<FailsafeElement<PubsubMessage, Row>, KV<String, PubsubMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(RowToPubsubMessageFn.class);

    static final long serialVersionUID = 234L;

    public RowToPubsubMessageFn() {}

    @Setup
    public void setup() throws Exception {}

    @ProcessElement
    public void processElement(
            @Element FailsafeElement<PubsubMessage, Row> received,
            OutputReceiver<KV<String, PubsubMessage>> out)
            throws Exception {
        Row row = received.getCurrentElement();
        try {
            TableRow tr = BigQueryUtils.toTableRow(row);
            ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
            TableRowJsonCoder.of().encode(tr, jsonStream, Context.OUTER);
            String str = new String(jsonStream.toByteArray(), StandardCharsets.UTF_8.name());

            // Message Attributes
            String entity = row.getSchema().getOptions().getValue("entity", String.class);

            ReadableDateTime eventTimestamp;
            if (row.getSchema().hasField("event_timestamp")
                    && row.getDateTime("event_timestamp") != null) {
                eventTimestamp = row.getDateTime("event_timestamp");
            } else {
                eventTimestamp = DateTime.now().withZone(DateTimeZone.UTC);
            }

            String eventUuid;
            if (row.getSchema().hasField("event_uuid") && row.getString("event_uuid") != null) {
                eventUuid = row.getString("event_uuid");
            } else {
                eventUuid = UUID.randomUUID().toString();
            }

            Map<String, String> attributes =
                    new HashMap<String, String>() {
                        static final long serialVersionUID = 2342534L;

                        {
                            put("timestamp", java.time.Instant.now().toString());
                            put("event_timestamp", String.valueOf(eventTimestamp.getMillis()));
                            put("event_uuid", eventUuid);
                            put("entity", entity);
                        }
                    };

            out.output(KV.of(entity, new PubsubMessage(str.getBytes("UTF-8"), attributes)));
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "RowToPubsubMessageFn.processElement()",
                    e.toString());
        }
    }
}
