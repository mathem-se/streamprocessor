package org.streamprocessor.core.transforms;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.Coder.Context;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.streamprocessor.core.utils.BigQueryUtils;
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
import org.streamprocessor.core.utils.CacheLoaderUtils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
// import com.google.cloud.bigquery.BigQueryOptions;


// import com.google.cloud.bigquery.BigQuery;

public class MapFloatTest {

    private static final Logger LOG = LoggerFactory.getLogger(MapFloatTest.class);

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
            .addField(Schema.Field.of("price", Schema.FieldType.DOUBLE))
            //.addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME))
            //.addNullableField("products", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
            .addMapField("productsMapFloat", Schema.FieldType.STRING, Schema.FieldType.DOUBLE)
            .addMapField("productsMapArray", Schema.FieldType.STRING, Schema.FieldType.array(Schema.FieldType.STRING))
            .addArrayField("products", Schema.FieldType.row(new Schema.Builder().addStringField("foo").build()))
            //.addRowField("struc", new Schema.Builder()
            //.addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME)).build())
            .build();

        // LOG.info(schema.toString());
        String linkedResource =
                    String.format(
                            "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                            "mathem-ml-datahem-test", "airbyte_test", "map_test");
        
        //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        //LOG.info("table schema bq: " + bigquery.getTable("airbyte_test", "map_test").getDefinition().getSchema().toString());

        //Schema schema = CacheLoaderUtils.getSchema(linkedResource);
        
        TableSchema ts = BigQueryUtils.toTableSchema(schema);
        LOG.info("table schema: " + ts.toString());
        //LOG.info("table schema BQ: " + tsBQ.toString());

        Schema brs = BigQueryUtils.fromTableSchema(ts, BigQueryUtils.SchemaConversionOptions.builder().setInferMaps(true).build());
        LOG.info(brs.toString());
        
        //String payload = "{\"Products\":{\"Init\":{\"Category\":null,\"Discount\":null,\"Attributes\":null,\"Url\":null,\"Name\":null,\"Department\":null,\"HasPharmacyDiff\":false,\"SoldBy\":null,\"Supplier\":null,\"StockInformation\":null,\"Availability\":null,\"ExternalSupplierCode\":null,\"Added\":null,\"Quantity\":0,\"ImageUrl\":null,\"ExternalUrl\":null,\"Disclaimer\":null,\"VatRate\":0,\"RecycleFee\":0,\"Brand\":null,\"Subtitle\":null,\"Price\":0,\"CategoryAncestry\":null,\"PharmacyInformation\":null,\"ExternalProductCode\":null,\"Id\":\"0\",\"Delivery\":null,\"EanCode\":null,\"ShowSoldBy\":false},\"127144\":{\"Category\":{\"Id\":\"kottfars---not\",\"Name\":\"Köttfärs - Nöt\"},\"Discount\":null,\"Attributes\":null,\"Url\":\"/varor/kottfars---not/notfars-12--800g-scan\",\"Name\":\"Nötfärs 12% 800g Scan\",\"Department\":{\"Id\":\"kott-o-chark\",\"Name\":\"Kött & Chark\"},\"HasPharmacyDiff\":false,\"SoldBy\":\"mathem\",\"Supplier\":{\"Id\":\"10607\",\"Name\":\"HK Scan Kött\"},\"StockInformation\":{\"Active\":true,\"OutOfStock\":false,\"AvailableQuantity\":null},\"Availability\":\"Available\",\"ExternalSupplierCode\":null,\"Added\":null,\"Quantity\":1,\"ImageUrl\":\"notfars-12--800g-scan-4.jpg\",\"ExternalUrl\":null,\"Disclaimer\":null,\"VatRate\":12,\"RecycleFee\":0,\"Brand\":{\"Id\":\"scan\",\"Name\":\"Scan\"},\"Subtitle\":null,\"Price\":97.95,\"CategoryAncestry\":[{\"Id\":\"kottfars---not\",\"Name\":\"Köttfärs - Nöt\"},{\"Id\":\"kottfars\",\"Name\":\"Köttfärs\"},{\"Id\":\"kott\",\"Name\":\"Kött\"},{\"Id\":\"kott-o-chark\",\"Name\":\"Kött & Chark\"}],\"PharmacyInformation\":null,\"ExternalProductCode\":\"207692002\",\"Id\":\"127144\",\"Delivery\":{\"AdvanceDeliveryMinimumOrder\":0,\"DeliverableWeekdays\":[],\"IsPickingCostIncluded\":false,\"CancelDaysBefore\":0,\"IsAdultProduct\":false,\"MinimumRequiredDeliveryDays\":0,\"IsSubscriptionProduct\":false,\"IsWine\":false,\"DeliveryRestrictions\":[],\"IsDeliveryCostIncluded\":false},\"EanCode\":\"7300207692002\",\"ShowSoldBy\":false}},\"Modified\":\"2023-02-21T15:37:32.2653538Z\",\"Address\":{\"CreationDate\":\"2022-02-03T14:19:03\",\"LastModifiedDate\":\"2022-12-20T14:00:18.290004+00:00\",\"AlternativePhoneMobile\":null,\"LegacyAddressId\":\"9725338\",\"FirstName\":\"Andreas\",\"PostalCode\":\"11734\",\"Latitude\":0,\"City\":\"Stockholm\",\"Longitude\":0,\"InHomeAvailable\":false,\"DoorCode\":null,\"AddressInfo\":null,\"PhoneMobile\":\"+46 73 912 16 32\",\"MemberId\":\"2392590\",\"CompanyName\":null,\"TruckRouteId\":0,\"AlternativePhoneHome\":null,\"RemovedDate\":null,\"PhoneHome\":null,\"AddressRow1\":\"Hornsbruksgatan 12\",\"Id\":\"d4e60122-4002-435b-b592-c15354473774\",\"LastName\":\"Fagerberg\",\"AddressType\":\"M\",\"AddressRow2\":null},\"OriginalDeliveryDate\":null,\"Discounts\":{\"Init\":{\"Percentage\":0,\"PercentageDeliveryFee\":0,\"Value\":0,\"Text\":null,\"ProductId\":null,\"Id\":\"0\",\"Code\":null,\"AmountLimit\":0}},\"Benefits\":null,\"event_timestamp\":\"2023-02-21T15:37:32.4701837Z\",\"Created\":\"2023-02-08T10:16:04.2961976Z\",\"MemberId\":\"2392590\",\"SocialSecurityNo\":null,\"NumberOfConfirmedOrderLines\":0,\"DeliveryTimeStatus\":{\"Status\":\"Pending\",\"Modified\":\"2023-02-21T15:37:32.1264911Z\",\"ExpiresAt\":null},\"Credit\":{\"CONFIRMED_ORDER_CREDIT\":0,\"USED_CREDIT\":0,\"AVAILABLE_CREDIT\":0},\"SubscriptionId\":null,\"StoreId\":\"20\",\"ChangingOrderId\":0,\"TotalPaidAmount\":0,\"UnavailableSubscriptionProducts\":{\"Init\":{\"Quantity\":0,\"ProductId\":null,\"ReasonCode\":null,\"Name\":null}},\"_metadata\":{\"processing_timestamp\":\"2023-02-21T15:37:55.702Z\",\"dynamodbEventId\":\"9c1fd374-5dca-45ab-ad83-8fc981ad98c0\",\"topic\":\"ecom-cart-service-CartEventTopic\",\"uuid\":\"2819cb25-c1f8-5a31-8e67-974b26840508\",\"operation\":\"MODIFY\",\"entity\":\"ecom-cart-service-CartEventTopic\",\"timestamp\":\"2023-02-21T15:37:32.477Z\",\"dynamodbPublished\":\"2023-02-21T15:37:32.4701837Z\"},\"CartStatus\":\"OPEN\",\"Bonuses\":{\"Init\":{\"Amount\":0,\"Id\":\"0\",\"Code\":null}},\"OriginalCartId\":null,\"RecurringOrder\":null,\"Fees\":{\"Init\":{\"Amount\":0,\"Id\":\"0\",\"VatRate\":0}},\"GiftVouchers\":{\"Init\":{\"Comments\":null,\"Value\":0,\"ValidFrom\":\"0001-01-01T00:00:00Z\",\"Text\":null,\"AlreadyUsed\":false,\"Id\":\"0\",\"Code\":null,\"ValidUntil\":\"0001-01-01T00:00:00Z\"}},\"LeaveOutsideDoor\":\"None\",\"DeliveryTime\":{\"DeliveryFeeLimits\":[],\"Added\":\"2023-02-21T15:37:32.1264759Z\",\"ClosingTime\":\"17:15:00\",\"DeliveryCost\":19,\"DeliveryTimeChangedByAdmin\":false,\"StartDate\":\"2023-02-21T18:00:00+01:00\",\"TruckRouteId\":4737,\"Type\":\"ExpressPlanned\",\"StoreId\":null,\"StoreRegion\":\"26\",\"ExtraDeliveryCost\":0,\"StopDate\":\"2023-02-21T19:00:00+01:00\",\"TimeId\":\"61917\",\"ClosingDate\":\"2023-02-21T00:00:00+01:00\",\"Id\":\"61917\",\"DeliveryRestrictions\":[]},\"Payment\":null,\"MemberGroupFees\":null,\"Revision\":474,\"MemberType\":\"P\",\"OriginalProducts\":{\"Init\":{\"Price\":0,\"ProductDiscountPrice\":null,\"Quantity\":0,\"Id\":\"0\",\"IsWine\":false,\"Name\":null}},\"Recipes\":{\"Init\":{\"RecipeProducts\":[],\"Added\":null,\"Portions\":0,\"ExternalRecipeId\":null,\"RecipeId\":null,\"ImageUrl\":null,\"Id\":\"0\",\"Url\":null,\"Name\":null}},\"OrderMessage\":null,\"Id\":\"1ec9a807-93cb-4c80-a691-996081c33df9\",\"Expires\":\"2023-02-28T15:37:32Z\"}";
        
        //JSONObject json = new JSONObject(payload);
        //LOG.info(json.toString());

        JSONObject json =
          new JSONObject()
              .put("price", 0)
              .put("products", new JSONArray().put(new JSONObject().put("foo", "bar")).put(new JSONObject().put("foo", "bar2")))
              .put("productsMapFloat", new JSONObject().put("foo", 0.0).put("hello", 1.0))
              .put("productsMapArray", new JSONObject().put("foo", new JSONArray().put("hello").put("world")));
              //.put("products", new JSONObject().put("foo", "bar").put("hello", "world"))
              //.put("struc", new JSONObject().put("lastname", "doe"));
        LOG.info(json.toString());


        //  Schema MAP_MAP_TYPE = Schema.builder().addMapField("map", Schema.FieldType.STRING, Schema.FieldType.DOUBLE).build();
        //  Row MAP_ROW = Row.withSchema(MAP_MAP_TYPE).addValues(ImmutableMap.of("test", 123.456, "test2", 12.345)).build();
        //  TableRow row = BigQueryUtils.toTableRow().apply(MAP_ROW);
        //  LOG.info(row.toString());

        // Row br = BigQueryUtils.toBeamRow(MAP_MAP_TYPE, row);
        // LOG.info(br.toString());

        TableRow tr = convertJsonToTableRow(json.toString());
        LOG.info("table row " + tr.toString());

        LOG.info("schema: " + schema.toString());

        Row br = BigQueryUtils.toBeamRow(schema, tr);
        LOG.info("beam row" + br.toString());
        

        // Row row = RowJsonUtils.jsonToRow(
        // RowJsonUtils.newObjectMapperWith(
        //   RowJsonDeserializer
        //     .forSchema(schema)
        //     .withNullBehavior(RowJsonDeserializer.NullBehavior.ACCEPT_MISSING_OR_NULL)), 
        //   json.toString());
      
        //   LOG.info(row.toString());

          
          
    }
}
