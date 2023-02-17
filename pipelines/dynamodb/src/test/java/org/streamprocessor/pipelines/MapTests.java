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

        // Schema schema =
        // new Schema.Builder()
        //     .addField(Schema.Field.of("firstname", Schema.FieldType.STRING))
        //     //.addNullableField("products", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        //     .addMapField("products", Schema.FieldType.STRING, Schema.FieldType.STRING)
        //     //.addArrayField("products", Schema.FieldType.row(new Schema.Builder().addStringField("key").addStringField("value").build()))
        //     .addRowField("struc", new Schema.Builder()
        //       .addField(Schema.Field.of("lastname", Schema.FieldType.STRING)).build())
        //     .build();
        // LOG.info(schema.toString());

        // TableSchema ts = BigQueryUtils.toTableSchema(schema);
        // LOG.info(ts.toString());

        // Schema brs = BigQueryUtils.fromTableSchema(ts, BigQueryUtils.SchemaConversionOptions.builder().setInferMaps(true).build());
        //   LOG.info(brs.toString());

        // JSONObject json =
        //   new JSONObject()
        //       .put("firstname", "Joe")
        //       .put("products", new JSONObject().put("foo", "bar").put("hello", "world"))
        //       .put("struc", new JSONObject().put("lastname", "doe"));
        // LOG.info(json.toString());

        // TableRow tr = convertJsonToTableRow(json.toString());
        // LOG.info(tr.toString());

        // Row bra = BigQueryUtils.toBeamRow(schema, tr);
        // LOG.info(bra.toString());

        // TableRow row = BigQueryUtils.toTableRow().apply(bra);
        // LOG.info(row.toString());

        

        Schema complex_schema =
        new Schema.Builder()
            //.addField(Schema.Field.of("order_id", Schema.FieldType.INT64))
            .addNullableField("order_id", Schema.FieldType.of(Schema.FieldType.INT64.getTypeName()))
            .addNullableField("products", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.row(new Schema.Builder().addStringField("brand").addDoubleField("price").build())))
            //.addMapField("products", Schema.FieldType.STRING, Schema.FieldType.row(new Schema.Builder().addStringField("brand").addDoubleField("price").build()))
            .build();
        LOG.info(complex_schema.toString());

        TableSchema complex_ts = BigQueryUtils.toTableSchema(complex_schema);
        LOG.info(complex_ts.toString());

        Schema complex_brs = BigQueryUtils.fromTableSchema(complex_ts, BigQueryUtils.SchemaConversionOptions.builder().setInferMaps(true).build());
          LOG.info(complex_brs.toString());

          JSONObject complex_json =
          new JSONObject()
              .put("order_id", 10)
              .put(
                "products", new JSONObject().put("milk", new JSONObject().put("brand","Arla").put("price", 15.00)));
        LOG.info(complex_json.toString());
     
        TableRow complex_tr = convertJsonToTableRow(complex_json.toString());
        LOG.info(complex_tr.toString());

        Row complex_bra = BigQueryUtils.toBeamRow(complex_schema, complex_tr);
        LOG.info(complex_bra.toString());

        TableRow complex_row = BigQueryUtils.toTableRow().apply(complex_bra);
        LOG.info(complex_row.toString());

    }
}
