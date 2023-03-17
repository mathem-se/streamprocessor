package org.streamprocessor.core.utils;

import static org.junit.Assert.assertEquals;

import com.google.cloud.datacatalog.v1beta1.ColumnSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

public class SchemaUtilsTest {

    @Test
    public void testFromDataCatalog() {
        Schema expectedBeamSchema =
                Schema.builder().addNullableField("id", Schema.FieldType.INT32).build();
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
