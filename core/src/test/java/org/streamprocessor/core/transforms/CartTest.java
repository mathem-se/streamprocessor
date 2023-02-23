package org.streamprocessor.core.transforms;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.Coder.Context;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.streamprocessor.core.utils.BqUtils;
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


// import com.google.cloud.bigquery.BigQuery;

public class CartTest {

    private static final Logger LOG = LoggerFactory.getLogger(CartTest.class);

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
        //     //.addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME))
        //     //.addNullableField("products", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        //     //.addMapField("products", Schema.FieldType.STRING, Schema.FieldType.row(new Schema.Builder().addStringField("foo").addStringField("bar").build()))
        //     .addArrayField("products", Schema.FieldType.row(new Schema.Builder().addStringField("foo").build()))
        //     //.addRowField("struc", new Schema.Builder()
        //     //.addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME)).build())
        //     .build();

        // LOG.info(schema.toString());
        String arrayString =
                    String.format(
                            "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                            "mathem-ml-datahem-test", "dynamodb", "ecom_cart_service_carteventtopic");
        
        Schema schema = CacheLoaderUtils.getSchema(arrayString);
        LOG.info("map row schema: " + schema.toString());
        TableSchema ts = BqUtils.toTableSchema(schema);
        LOG.info("table schema: " + ts.toString());

        // String payload = "{\"Products\":{\"Init\":{\"Category\":null,\"Discount\":null,\"Attributes\":null,\"Url\":null,\"Name\":null,\"Department\":null,\"HasPharmacyDiff\":false,\"SoldBy\":null,\"Supplier\":null,\"StockInformation\":null,\"Availability\":null,\"ExternalSupplierCode\":null,\"Added\":null,\"Quantity\":0,\"ImageUrl\":null,\"ExternalUrl\":null,\"Disclaimer\":null,\"VatRate\":0,\"RecycleFee\":0,\"Brand\":null,\"Subtitle\":null,\"Price\":0,\"CategoryAncestry\":null,\"PharmacyInformation\":null,\"ExternalProductCode\":null,\"Id\":\"0\",\"Delivery\":null,\"EanCode\":null,\"ShowSoldBy\":false}},\"Modified\":null,\"Address\":null,\"OriginalDeliveryDate\":null,\"Discounts\":{\"Init\":{\"Percentage\":0,\"PercentageDeliveryFee\":0,\"Value\":0,\"Text\":null,\"ProductId\":null,\"Id\":\"0\",\"Code\":null,\"AmountLimit\":0}},\"Benefits\":null,\"event_timestamp\":\"2023-02-21T21:44:46.6105427Z\",\"Created\":\"2023-02-21T21:44:46.2066585Z\",\"MemberId\":null,\"SocialSecurityNo\":null,\"NumberOfConfirmedOrderLines\":0,\"DeliveryTimeStatus\":{\"Status\":\"None\",\"Modified\":\"2023-02-21T21:44:46.2067023Z\",\"ExpiresAt\":null},\"Credit\":{\"CONFIRMED_ORDER_CREDIT\":0,\"USED_CREDIT\":0,\"AVAILABLE_CREDIT\":0},\"SubscriptionId\":null,\"StoreId\":\"23\",\"ChangingOrderId\":0,\"TotalPaidAmount\":0,\"UnavailableSubscriptionProducts\":{\"Init\":{\"Quantity\":0,\"ProductId\":null,\"ReasonCode\":null,\"Name\":null}},\"_metadata\":{\"processing_timestamp\":\"2023-02-22T13:10:41.092Z\",\"dynamodbEventId\":\"265126e1-5ad6-457e-b76b-a91fc2d35a7b\",\"topic\":\"ecom-cart-service-CartEventTopic\",\"uuid\":\"734e91af-87be-5673-8f33-cdc0ba30e8a7\",\"operation\":\"MODIFY\",\"entity\":\"ecom-cart-service-CartEventTopic\",\"timestamp\":\"2023-02-21T21:44:46.653Z\",\"dynamodbPublished\":\"2023-02-21T21:44:46.6105427Z\"},\"CartStatus\":\"OPEN\",\"Bonuses\":{\"Init\":{\"Amount\":0,\"Id\":\"0\",\"Code\":null}},\"OriginalCartId\":null,\"RecurringOrder\":null,\"Fees\":{\"Init\":{\"Amount\":0,\"Id\":\"0\",\"VatRate\":0}},\"GiftVouchers\":{\"Init\":{\"Comments\":null,\"Value\":0,\"ValidFrom\":\"0001-01-01T00:00:00\",\"Text\":null,\"AlreadyUsed\":false,\"Id\":\"0\",\"Code\":null,\"ValidUntil\":\"0001-01-01T00:00:00\"}},\"LeaveOutsideDoor\":null,\"DeliveryTime\":null,\"Payment\":null,\"MemberGroupFees\":null,\"Revision\":1,\"MemberType\":null,\"OriginalProducts\":{\"Init\":{\"Price\":0,\"ProductDiscountPrice\":null,\"Quantity\":0,\"Id\":\"0\",\"IsWine\":false,\"Name\":null}},\"Recipes\":{\"Init\":{\"RecipeProducts\":[],\"Added\":null,\"Portions\":0,\"ExternalRecipeId\":null,\"RecipeId\":null,\"ImageUrl\":null,\"Id\":\"0\",\"Url\":null,\"Name\":null}},\"OrderMessage\":null,\"Id\":\"1a72fbed-74c6-407d-8162-081838d6e41f\",\"Expires\":\"2023-02-28T21:44:46Z\"}";
        //String payload = "{ \"Products\": { \"Init\": { \"Category\": null, \"Discount\": null, \"Attributes\": null, \"Url\": null, \"Name\": null, \"Department\": null, \"HasPharmacyDiff\": false, \"SoldBy\": null, \"Supplier\": null, \"StockInformation\": null, \"Availability\": null, \"ExternalSupplierCode\": null, \"Added\": null, \"Quantity\": 0, \"ImageUrl\": null, \"ExternalUrl\": null, \"Disclaimer\": null, \"VatRate\": 0, \"RecycleFee\": 0, \"Brand\": null, \"Subtitle\": null, \"Price\": 0, \"CategoryAncestry\": null, \"PharmacyInformation\": null, \"ExternalProductCode\": null, \"Id\": \"0\", \"Delivery\": null, \"EanCode\": null, \"ShowSoldBy\": false } } }";
        // String payload = "{ \"Products\": { \"Init\": { \"Category\": null, \"Discount\": null, \"Attributes\": null, \"Url\": null, \"Name\": null, \"Department\": null, \"HasPharmacyDiff\": false, \"SoldBy\": null, \"Supplier\": null, \"StockInformation\": null, \"Availability\": null, \"ExternalSupplierCode\": null, \"Added\": null, \"Quantity\": 0, \"ImageUrl\": null, \"ExternalUrl\": null, \"Disclaimer\": null, \"VatRate\": 0, \"RecycleFee\": 0, \"Brand\": null, \"Subtitle\": null, \"Price\": 0, \"CategoryAncestry\": null, \"PharmacyInformation\": null, \"ExternalProductCode\": null, \"Id\": \"0\", \"Delivery\": null, \"EanCode\": null, \"ShowSoldBy\": false } }, \"Modified\": null, \"Address\": null, \"OriginalDeliveryDate\": null, \"Discounts\": { \"Init\": { \"Percentage\": 0, \"PercentageDeliveryFee\": 0, \"Value\": 0, \"Text\": null, \"ProductId\": null, \"Id\": \"0\", \"Code\": null, \"AmountLimit\": 0 } }, \"Benefits\": null, \"event_timestamp\": \"2023-02-21T21:44:46.6105427Z\", \"Created\": \"2023-02-21T21:44:46.2066585Z\", \"MemberId\": null, \"SocialSecurityNo\": null, \"NumberOfConfirmedOrderLines\": 0, \"DeliveryTimeStatus\": { \"Status\": \"None\", \"Modified\": \"2023-02-21T21:44:46.2067023Z\", \"ExpiresAt\": null }, \"Credit\": { \"CONFIRMED_ORDER_CREDIT\": 0, \"USED_CREDIT\": 0, \"AVAILABLE_CREDIT\": 0 }, \"SubscriptionId\": null, \"StoreId\": \"23\", \"ChangingOrderId\": 0, \"TotalPaidAmount\": 0, \"UnavailableSubscriptionProducts\": { \"Init\": { \"Quantity\": 0, \"ProductId\": null, \"ReasonCode\": null, \"Name\": null } } }";
        // String payload = "        { \"Modified\": null, \"Address\": null, \"OriginalDeliveryDate\": null, \"Discounts\": { \"Init\": { \"Percentage\": 0, \"PercentageDeliveryFee\": 0, \"Value\": 0, \"Text\": null, \"ProductId\": null, \"Id\": \"0\", \"Code\": null, \"AmountLimit\": 0 } }, \"Benefits\": null, \"event_timestamp\": \"2023-02-21T21:44:46.6105427Z\", \"Created\": \"2023-02-21T21:44:46.2066585Z\", \"MemberId\": null, \"SocialSecurityNo\": null, \"NumberOfConfirmedOrderLines\": 0, \"DeliveryTimeStatus\": { \"Status\": \"None\", \"Modified\": \"2023-02-21T21:44:46.2067023Z\", \"ExpiresAt\": null } }";
        String payload = "{ \"SubscriptionId\": null, \"StoreId\": \"23\", \"ChangingOrderId\": 0, \"TotalPaidAmount\": 0, \"UnavailableSubscriptionProducts\": { \"Init\": { \"Quantity\": 0, \"ProductId\": null, \"ReasonCode\": null, \"Name\": null } } }";

        // String payload = "{ \"Modified\": null, \"Address\": null, \"OriginalDeliveryDate\": null, \"Discounts\": { \"Init\": { \"Percentage\": 0, \"PercentageDeliveryFee\": 0, \"Value\": 0, \"Text\": null, \"ProductId\": null, \"Id\": \"0\", \"Code\": null, \"AmountLimit\": 0 } }, \"Benefits\": null, \"event_timestamp\": \"2023-02-21T21:44:46.6105427Z\", \"Created\": \"2023-02-21T21:44:46.2066585Z\", \"MemberId\": null, \"SocialSecurityNo\": null, \"NumberOfConfirmedOrderLines\": 0, \"DeliveryTimeStatus\": { \"Status\": \"None\", \"Modified\": \"2023-02-21T21:44:46.2067023Z\", \"ExpiresAt\": null }, \"Credit\": { \"CONFIRMED_ORDER_CREDIT\": 0, \"USED_CREDIT\": 0, \"AVAILABLE_CREDIT\": 0 }, \"SubscriptionId\": null, \"StoreId\": \"23\", \"ChangingOrderId\": 0, \"TotalPaidAmount\": 0, \"UnavailableSubscriptionProducts\": { \"Init\": { \"Quantity\": 0, \"ProductId\": null, \"ReasonCode\": null, \"Name\": null } } }";

        TableRow tr = convertJsonToTableRow(payload);
        LOG.info("table row " + tr.toString());

        LOG.info("schema: " + schema.toString());

        Row br = BqUtils.toBeamRow(schema, tr);
        
        LOG.info("beam row" + br);
          
    }
}
