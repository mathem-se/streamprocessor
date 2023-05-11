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

package org.streamprocessor.core.utils;

import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.TagField;
import com.google.common.cache.CacheLoader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CacheLoaderUtils implements Serializable {

    static final long serialVersionUID = 89422138932L;
    private static final Logger LOG = LoggerFactory.getLogger(CacheLoaderUtils.class);

    public static JSONObject getDataContract(String endpoint) {
        try {
            JSONObject dataContract = DataContractUtils.getDataContract(endpoint);
            return dataContract;

        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "CacheLoaderUtils.getDataContract()",
                    e.toString());
            return null;
        }
    }

    public static Schema getSchema(String linkedResource) {
        try (DataCatalogClient dataCatalogClient = DataCatalogClient.create()) {
            // Get data catalog schema
            LookupEntryRequest request =
                    LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();
            Entry entry = dataCatalogClient.lookupEntry(request);

            // Add the entity as a Row option
            String entity = linkedResource.substring(linkedResource.lastIndexOf("/") + 1);
            String[] linkedResourceComponents = linkedResource.split("/");
            String datasetId =
                    linkedResourceComponents[
                            Arrays.asList(linkedResourceComponents).indexOf("datasets") + 1];

            // Message level options
            Schema.Options schemaOptions =
                    Schema.Options.builder()
                            .setOption("entity", Schema.FieldType.STRING, entity)
                            .setOption("datasetId", Schema.FieldType.STRING, datasetId)
                            .build();

            // Iterate tags and store as Row field options (String, Enum, Bool)
            Map<String, Schema.Options> optionsMap = new HashMap<String, Schema.Options>();
            for (Tag tag : dataCatalogClient.listTags(entry.getName()).iterateAll()) {
                Schema.Options.Builder fieldSchemaOptionsBuilder = Schema.Options.builder();
                Map<String, TagField> fieldsMap = tag.getFieldsMap();
                for (Map.Entry<String, TagField> tagField : fieldsMap.entrySet()) {
                    if (tagField.getValue().hasStringValue()) {
                        fieldSchemaOptionsBuilder.setOption(
                                tagField.getKey(),
                                Schema.FieldType.STRING,
                                tagField.getValue().getStringValue());
                    }
                    if (tagField.getValue().hasEnumValue()) {
                        fieldSchemaOptionsBuilder.setOption(
                                tagField.getKey(),
                                Schema.FieldType.STRING,
                                tagField.getValue().getEnumValue().getDisplayName());
                    }
                    if (tagField.getValue().hasBoolValue()) {
                        fieldSchemaOptionsBuilder.setOption(
                                tagField.getKey(),
                                Schema.FieldType.BOOLEAN,
                                tagField.getValue().getBoolValue());
                    }
                }
                optionsMap.put(tag.getColumn().toLowerCase(), fieldSchemaOptionsBuilder.build());
            }

            // Data catalog entry schema to list of beam Row schema fields
            List<Schema.Field> fieldList =
                    SchemaUtils.fromDataCatalog(entry.getSchema()).getFields();

            // Add field options to fields
            List<Schema.Field> taggedFieldList = new ArrayList<Schema.Field>();
            for (Schema.Field field : fieldList) {
                if (optionsMap.containsKey(field.getName().toLowerCase())) {
                    taggedFieldList.add(
                            field.withOptions(optionsMap.get(field.getName().toLowerCase())));
                } else {
                    taggedFieldList.add(field);
                }
            }

            // Create a Row schema with message and field level options
            Schema schema =
                    Schema.builder().addFields(taggedFieldList).build().withOptions(schemaOptions);
            return schema;
        } catch (PermissionDeniedException e) {
            LOG.warn(
                    "exception[{}] step[{}] details[{}]",
                    "PermissionDeniedException",
                    "CacheLoaderUtils.getSchema()",
                    e.toString());
            return Schema.builder().build();
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "CacheLoaderUtils.getSchema()",
                    e.toString());
            return Schema.builder().build();
        }
    }

    public static CacheLoader<String, Schema> schemaCacheLoader() {
        return new CacheLoader<String, Schema>() {
            @Override
            public Schema load(String sqlResource) {
                try {
                    return getSchema(sqlResource);
                } catch (Exception e) {
                    LOG.error(
                            "exception[{}] step[{}] details[{}]",
                            e.getClass().getName(),
                            "CacheLoaderUtils.schemaCacheLoader()",
                            e.toString());
                    return null;
                }
            }
        };
    }
}
