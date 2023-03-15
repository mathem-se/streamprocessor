package org.streamprocessor.core.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import static org.junit.Assert.assertEquals;

import com.google.cloud.datacatalog.v1beta1.ColumnSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

public class SchemaUtilsTest {


    @Test
    public void testFromDataCatalog() {
        Schema expectedBeamSchema = Schema.builder().addNullableField("id", Schema.FieldType.INT32).build();
        com.google.cloud.datacatalog.v1beta1.Schema testSCHEMA =
                com.google.cloud.datacatalog.v1beta1.Schema.newBuilder()
                        .addColumns(
                                ColumnSchema.newBuilder()
                                        .setColumn("id")
                                        .setType("INT32")
                                        .setMode("NULLABLE")
                                        .build())
                        .build();
        Schema schema = SchemaUtils.fromDataCatalog(testSCHEMA);
        assertEquals(expectedBeamSchema, schema);
    }
}
