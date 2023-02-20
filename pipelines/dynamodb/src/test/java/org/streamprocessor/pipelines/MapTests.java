package org.streamprocessor.pipelines;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.Coder.Context;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.streamprocessor.pipelines.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.SchemaConversionOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class MapTests {

    private static final Logger LOG = LoggerFactory.getLogger(MapTests.class);

    public static TableRow convertJsonToTableRow(String json) {
      TableRow row;
      // Parse the JSON into a {@link TableRow} object.
      try (InputStream inputStream =
          new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
          row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
        
  
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize json to table row: " + json, e);
      }
  
      return row;
    }
      
    public static void main(String[] args) {

        Schema schema =
        new Schema.Builder()
            .addField(Schema.Field.of("firstname", Schema.FieldType.STRING))
            .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME))
            //.addNullableField("products", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
            .addMapField("products", Schema.FieldType.STRING, Schema.FieldType.STRING)
            //.addArrayField("products", Schema.FieldType.row(new Schema.Builder().addStringField("key").addStringField("value").build()))
            .addRowField("struc", new Schema.Builder()
              .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME)).build())
            .build();
        LOG.info(schema.toString());

        TableSchema ts = BigQueryUtils.toTableSchema(schema);
        LOG.info(ts.toString());

        Schema brs = BigQueryUtils.fromTableSchema(ts, BigQueryUtils.SchemaConversionOptions.builder().setInferMaps(true).build());
          LOG.info(brs.toString());

        JSONObject json =
          new JSONObject()
              .put("firstname", "Joe")
              .put("timestamp", java.time.Instant.now().toString())
              .put("products", new JSONObject().put("foo", "bar"))
              .put("struc", new JSONObject().put("timestamp", "2023-02-20T13:27:14.9484233Z"));
        LOG.info(json.toString());

        //  Schema MAP_MAP_TYPE = Schema.builder().addMapField("map", Schema.FieldType.STRING, Schema.FieldType.DOUBLE).build();
        //  Row MAP_ROW = Row.withSchema(MAP_MAP_TYPE).addValues(ImmutableMap.of("test", 123.456, "test2", 12.345)).build();
        //  TableRow row = BigQueryUtils.toTableRow().apply(MAP_ROW);
        //  LOG.info(row.toString());

        // Row br = BigQueryUtils.toBeamRow(MAP_MAP_TYPE, row);
        // LOG.info(br.toString());

        TableRow tr = convertJsonToTableRow(json.toString());
        LOG.info(tr.toString());

        Row br = BigQueryUtils.toBeamRow(schema, tr);
        LOG.info(br.toString());

        // Row row = RowJsonUtils.jsonToRow(
        // RowJsonUtils.newObjectMapperWith(
        //   RowJsonDeserializer
        //     .forSchema(schema)
        //     .withNullBehavior(RowJsonDeserializer.NullBehavior.ACCEPT_MISSING_OR_NULL)), 
        //   json.toString());
      
        //   LOG.info(row.toString());

          
          
    }
}
