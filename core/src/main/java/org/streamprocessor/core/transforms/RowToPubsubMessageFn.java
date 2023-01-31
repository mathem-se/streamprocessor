/* (C)2021 */
package org.streamprocessor.core.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowToPubsubMessageFn extends DoFn<Row, KV<String, PubsubMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(RowToPubsubMessageFn.class);

    static final long serialVersionUID = 234L;

    public RowToPubsubMessageFn() {}

    @Setup
    public void setup() throws Exception {}

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<KV<String, PubsubMessage>> out)
            throws Exception {
        try {
            RowJson.RowJsonSerializer jsonSerializer =
                    RowJson.RowJsonSerializer.forSchema(row.getSchema()).withDropNullsOnWrite(true);
            ObjectMapper objectMapper = RowJsonUtils.newObjectMapperWith(jsonSerializer);
            String str = RowJsonUtils.rowToJson(objectMapper, row);
            // LOG.info(str);

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
            LOG.error("RowToPubsubMessage: " + e.getMessage());
        }
    }
}
