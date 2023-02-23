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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.utils.CacheLoaderUtils;
import org.streamprocessor.core.utils.BigQueryUtils;
import com.google.api.services.bigquery.model.TableRow;

public class SerializeMessageToRowFn extends DoFn<PubsubMessage, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(SerializeMessageToRowFn.class);
    private static final Counter schemaCacheMissesCounter =
            Metrics.counter("SerializeMessageToRowFn", "schemaCacheMisses");
    private static final Counter schemaCacheCallsCounter =
            Metrics.counter("SerializeMessageToRowFn", "schemaCacheCalls");
    static final long serialVersionUID = 234L;
    private static final com.github.benmanes.caffeine.cache.LoadingCache<String, Schema>
            schemaCache =
                    Caffeine.newBuilder()
                            .maximumSize(1000)
                            .refreshAfterWrite(5, TimeUnit.MINUTES)
                            .build(k -> getCachedSchema(k));

    TupleTag<Row> successTag;
    TupleTag<PubsubMessage> deadLetterTag;
    String unknownFieldLogger;
    String format;
    String projectId;
    String datasetId;
    float ratio;

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static Set<String> getAllKeys(Schema schema) {
        Set<String> keySet = new HashSet<String>();
        return getAllKeys(schema.getFields(), keySet, "");
    }

    private static Set<String> getAllKeys(
            List<Schema.Field> fields, Set<String> keySet, String prefix) {
        try {
            for (Schema.Field field : fields) {
                keySet.add(prefix + field.getName());
                if (field.getType().getTypeName().equals(Schema.TypeName.ROW)) {
                    getAllKeys(
                            field.getType().getRowSchema().getFields(),
                            keySet,
                            prefix + field.getName() + ".");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("getAllKeys: " + e.getMessage());
        }
        return keySet;
    }

    private static Set<String> getAllKeys(JSONObject jsonObject) {
        Set<String> keySet = new HashSet<String>();
        return getAllKeys(jsonObject, keySet, "");
    }

    private static Set<String> getAllKeys(
            JSONObject jsonObject, Set<String> keySet, String prefix) {
        try {
            for (String key : jsonObject.keySet()) {
                keySet.add(prefix + key);
                if (jsonObject.get(key).getClass() == JSONObject.class) {
                    getAllKeys(jsonObject.getJSONObject(key), keySet, key + ".");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("getAllKeys: " + e.getMessage());
        }
        return keySet;
    }

    private class NoSchemaException extends Exception {

        private NoSchemaException(String errorMessage) {
            super(errorMessage);
        }
    }

    private static Schema getCachedSchema(String ref) {
        schemaCacheMissesCounter.inc();
        return CacheLoaderUtils.getSchema(ref);
    }

    public SerializeMessageToRowFn(
            TupleTag<Row> successTag,
            TupleTag<PubsubMessage> deadLetterTag,
            String projectId,
            String datasetId,
            float ratio) {
        this.successTag = successTag;
        this.deadLetterTag = deadLetterTag;
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.ratio = ratio;
    }

    public SerializeMessageToRowFn(
            TupleTag<Row> successTag,
            TupleTag<PubsubMessage> deadLetterTag,
            String projectId,
            String datasetId) {
        this.successTag = successTag;
        this.deadLetterTag = deadLetterTag;
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.ratio = 0.001f;
    }

    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
  
  
        } catch (IOException e) {
          LOG.info("could not parse json: " + json);
          throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
  
        return row;
      }


    @ProcessElement
    public void processElement(@Element PubsubMessage received, MultiOutputReceiver out) 
            throws Exception {
        LOG.info("received message: " + received.getPayload());
        String entity = received.getAttribute("entity").replace("-", "_").toLowerCase();
        String payload = new String(received.getPayload(), StandardCharsets.UTF_8);

        @Nullable Map<String, String> attributesMap = received.getAttributeMap();
        try {
            String linkedResource =
                    String.format(
                            "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                            projectId, datasetId, entity);
            schemaCacheCallsCounter.inc();
            Schema schema = schemaCache.get(linkedResource);
            SplittableRandom random = new SplittableRandom();
            if (schema.getFieldCount() == 0) {
                throw new NoSchemaException("Schema for " + linkedResource + " doesn't exist");
            }

            JSONObject json = new JSONObject(payload);

            if (json.isNull("event_timestamp")) {
                json.put("event_timestamp", DateTime.now().withZone(DateTimeZone.UTC).toString());
            }
            JSONObject attributes = new JSONObject(received.getAttributeMap());
            json.put("_metadata", attributes);

            // Identify unmapped fields in payload.
            // Sample ratio to check for differences
            if (random.nextInt(100) < ratio * 100) {
                Set<String> jsonKeySet = getAllKeys(json);
                Set<String> schemaKeySet = getAllKeys(schema);
                if (jsonKeySet.size() > schemaKeySet.size()) {
                    jsonKeySet.removeAll(schemaKeySet);
                    String unmappedFields = String.join(",", jsonKeySet);
                    LOG.warn(
                            entity
                                    + " unmapped fields: "
                                    + unmappedFields
                                    + " - payload: "
                                    + json.toString());
                }
            }

            TableRow tr = convertJsonToTableRow(json.toString());

            Row row = BigQueryUtils.toBeamRow(schema, tr);
            out.get(successTag).output(row);
        } catch (Exception e) {
            LOG.error(entity + ": " + e.toString());
            // TODO:
            // instead, pass the following to deadletter: original_payload, status, error_message
            // can't put in unmodifiable map
            // attributesMap.put("error_reason", StringUtils.left(e.toString(), 1024));
            out.get(deadLetterTag)
                    .output(new PubsubMessage(payload.getBytes("UTF-8"), attributesMap));
        }
    }
}