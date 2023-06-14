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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.helpers.FailsafeElement;
import org.streamprocessor.core.utils.CustomExceptionsUtils;

public class DeIdentifyFn extends DoFn<FailsafeElement<Row>, FailsafeElement<Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeIdentifyFn.class);
    private static final Counter tokenCacheMissesCounter =
            Metrics.counter("DeIdentifyFn", "tokenCacheMisses");
    private static final Counter tokenCacheCallsCounter =
            Metrics.counter("DeIdentifyFn", "tokenCacheCalls");
    private static Random random = new Random();

    static final long serialVersionUID = 234L;
    private static final com.github.benmanes.caffeine.cache.Cache<String, Object> tokenCache =
            Caffeine.newBuilder()
                    .maximumSize(10000)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build();

    private Firestore db;
    String firestoreProjectId;

    TupleTag<FailsafeElement<Row>> successTag;
    TupleTag<FailsafeElement<Row>> failureTag;

    public static Object getFieldToken(String tokenFullRef, Firestore db) {
        try {
            tokenCacheMissesCounter.inc();
            String[] refs = tokenFullRef.split("#");
            String federatedEntity = refs[0];
            String federatedIdentity = refs[1];
            String fieldName = refs[2];
            String fieldValue = refs[3];
            String fieldType = refs[4];

            DocumentReference federatedTokenRef =
                    db.collection("FederatedTokens")
                            .document(federatedEntity + "#" + federatedIdentity);
            DocumentReference fieldTokenRef =
                    federatedTokenRef
                            .collection("FieldTokens")
                            .document(fieldName + "#" + fieldValue);
            DocumentSnapshot fieldTokenDocument = fieldTokenRef.get().get();
            String timestamp = Instant.now().toString();

            // Document exists, return token
            if (fieldTokenDocument.exists()) {
                Object token = fieldTokenDocument.get("token");
                return token;
            }
            // Document doesn't exist, create a new using transaction.
            else {
                // run a transaction
                ApiFuture<Object> futureTransaction =
                        db.runTransaction(
                                transaction -> {
                                    DocumentSnapshot fieldSnapshot =
                                            transaction.get(fieldTokenRef).get();
                                    if (fieldSnapshot.exists()) {
                                        return fieldSnapshot.get("token");
                                    } else {
                                        String federatedEntityToken = UUID.randomUUID().toString();
                                        DocumentSnapshot federatedSnapshot =
                                                transaction.get(federatedTokenRef).get();
                                        if (!federatedSnapshot.exists()) {
                                            Map<String, Object> federatedEntityData =
                                                    new HashMap<>();
                                            federatedEntityData.put(
                                                    "identity", federatedEntityToken);
                                            federatedEntityData.put("value", federatedIdentity);
                                            federatedEntityData.put("createdAt", timestamp);
                                            transaction.set(federatedTokenRef, federatedEntityData);
                                        } else {
                                            federatedEntityToken =
                                                    federatedSnapshot.getString("identity");
                                        }
                                        Map<String, Object> fieldData = new HashMap<>();
                                        Object fieldToken;
                                        switch (fieldType) {
                                            case "INT32":
                                                fieldToken = random.nextInt(Integer.MAX_VALUE);
                                                fieldData.put("token", (int) fieldToken);
                                                fieldData.put(
                                                        "value", Integer.parseInt(fieldValue));
                                                break;
                                            case "INT64":
                                                fieldToken =
                                                        (long) (Math.random() * Long.MAX_VALUE);
                                                fieldData.put("token", (long) fieldToken);
                                                fieldData.put("value", Long.parseLong(fieldValue));
                                                break;
                                            case "DOUBLE":
                                                fieldToken =
                                                        Double.valueOf(
                                                                String.format(
                                                                        Locale.US,
                                                                        "%.3f",
                                                                        random.nextDouble()
                                                                                * 2147483d));
                                                fieldData.put("token", (double) fieldToken);
                                                fieldData.put("value", Double.valueOf(fieldValue));
                                                break;
                                                // String as default
                                            default:
                                                if (fieldValue.equals(federatedIdentity)) {
                                                    fieldToken = federatedEntityToken;
                                                } else {
                                                    fieldToken = UUID.randomUUID().toString();
                                                }
                                                fieldData.put("token", (String) fieldToken);
                                                fieldData.put("value", fieldValue);
                                                break;
                                        }
                                        fieldData.put("identity", federatedEntityToken);
                                        fieldData.put("entity", federatedEntity);
                                        fieldData.put("createdAt", timestamp);
                                        transaction.set(fieldTokenRef, fieldData);
                                        return fieldToken;
                                    }
                                });
                return futureTransaction.get();
            }
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}] guess[race condition?]",
                    e.getClass().getName(),
                    "DeIdentifyFn.getFieldToken()",
                    e.toString());
            throw new RuntimeException(
                    e.toString()); // DEBATABLE: Why through new and wrapped in a RuntimeException?
        }
    }

    public DeIdentifyFn(
            String firestoreProjectId,
            TupleTag<FailsafeElement<Row>> successTag,
            TupleTag<FailsafeElement<Row>> failureTag) {
        this.firestoreProjectId = firestoreProjectId;
        this.successTag = successTag;
        this.failureTag = failureTag;
    }

    @Setup
    public void setup() throws Exception {
        if (db == null) {
            LOG.info(
                    "DeIdentify setup(), initialize Firestore client. Thread: "
                            + Thread.currentThread().getId());
            if (firestoreProjectId != null) {
                db =
                        FirestoreOptions.newBuilder()
                                .setProjectId(firestoreProjectId)
                                .build()
                                .getService();
            } else {
                db = FirestoreOptions.getDefaultInstance().getService();
            }
        }
    }

    @ProcessElement
    public void processElement(@Element FailsafeElement<Row> received, MultiOutputReceiver out)
            throws Exception {
        FailsafeElement<Row> outputElement;
        Row outputRow;
        Row row = received.getNewElement();

        try {
            org.apache.beam.sdk.values.Row.FieldValueBuilder rowBuilder = Row.fromRow(row);
            Schema beamSchema = row.getSchema();
            Map<String, String> federatedEntityIds = new HashMap<String, String>();
            // Find identifier fields in message
            for (int i = 0; i < beamSchema.getFieldCount(); ++i) {
                Schema.Field field = beamSchema.getField(i);
                if (field.getOptions().hasOption("federated_identifier")) {
                    String federatedEntity =
                            field.getOptions().getValue("federated_identifier", String.class);
                    federatedEntityIds.put(federatedEntity, row.getString(i));
                }
            }

            for (int i = 0; i < beamSchema.getFieldCount(); ++i) {
                Schema.Field field = beamSchema.getField(i);
                String fieldName = field.getName();
                if ((field.getOptions().hasOption("federated_identifier")
                                && field.getOptions().getValue("tokenize", Boolean.class))
                        || (field.getOptions().hasOption("referenced_federated_identifier"))) {
                    tokenCacheCallsCounter.inc();
                    String federatedEntityRef;
                    if (field.getOptions().hasOption("federated_identifier")) {
                        federatedEntityRef =
                                field.getOptions().getValue("federated_identifier", String.class);
                    } else if (field.getOptions().hasOption("referenced_federated_identifier")) {
                        federatedEntityRef =
                                field.getOptions()
                                        .getValue("referenced_federated_identifier", String.class);
                    } else {
                        throw new CustomExceptionsUtils.MissingIdentifierException(
                                "Federated identifier missing");
                    }
                    String federatedIdentity = federatedEntityIds.get(federatedEntityRef);
                    if (federatedIdentity != null) {
                        if (field.getType().getTypeName().equals(Schema.TypeName.STRING)) {
                            String fieldValue = row.getString(i);
                            String fieldType = "STRING";
                            String fieldToken =
                                    (String)
                                            tokenCache.get(
                                                    federatedEntityRef
                                                            + "#"
                                                            + federatedIdentity
                                                            + "#"
                                                            + fieldName
                                                            + "#"
                                                            + fieldValue
                                                            + "#"
                                                            + fieldType,
                                                    k -> getFieldToken(k, db));
                            rowBuilder.withFieldValue(i, fieldToken);
                        } else if (field.getType().getTypeName().equals(Schema.TypeName.INT32)) {
                            int fieldValue = row.getInt32(i);
                            String fieldType = "INT32";
                            int fieldToken =
                                    (int)
                                            tokenCache.get(
                                                    federatedEntityRef
                                                            + "#"
                                                            + federatedIdentity
                                                            + "#"
                                                            + fieldName
                                                            + "#"
                                                            + fieldValue
                                                            + "#"
                                                            + fieldType,
                                                    k -> getFieldToken(k, db));
                            rowBuilder.withFieldValue(i, fieldToken);
                        } else if (field.getType().getTypeName().equals(Schema.TypeName.INT64)) {
                            long fieldValue = row.getInt64(i);
                            String fieldType = "INT64";
                            long fieldToken =
                                    (long)
                                            tokenCache.get(
                                                    federatedEntityRef
                                                            + "#"
                                                            + federatedIdentity
                                                            + "#"
                                                            + fieldName
                                                            + "#"
                                                            + String.valueOf(fieldValue)
                                                            + "#"
                                                            + fieldType,
                                                    k -> getFieldToken(k, db));
                            rowBuilder.withFieldValue(i, fieldToken);
                        } else if (field.getType().getTypeName().equals(Schema.TypeName.DOUBLE)) {
                            double fieldValue = row.getDouble(i);
                            String fieldType = "DOUBLE";
                            double fieldToken =
                                    (double)
                                            tokenCache.get(
                                                    federatedEntityRef
                                                            + "#"
                                                            + federatedIdentity
                                                            + "#"
                                                            + fieldName
                                                            + "#"
                                                            + fieldValue
                                                            + "#"
                                                            + fieldType,
                                                    k -> getFieldToken(k, db));
                            rowBuilder.withFieldValue(i, fieldToken);
                        }
                    }
                }
            }
            outputRow = rowBuilder.build();

            outputElement = new FailsafeElement<>(received.getOriginalElement(), outputRow);
            out.get(successTag).output(outputElement);

        } catch (Exception e) {
            outputElement =
                    new FailsafeElement<>(
                            received.getOriginalElement(),
                            row,
                            "DeIdentifyFn.processElement()",
                            e.getClass().getName(),
                            e);

            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    outputElement.getException(),
                    outputElement.getPipelineStep(),
                    outputElement.getExceptionDetails());

            // TODO: remove?
            // throw e;

            out.get(failureTag).output(outputElement);
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        LOG.info(
                "DeIdentify tearDown(), close Firestore client. Thread: "
                        + Thread.currentThread().getId());
        db.shutdownNow();
        db.close();
    }
}
