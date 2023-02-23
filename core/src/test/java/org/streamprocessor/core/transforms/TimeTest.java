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
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

// import com.google.cloud.bigquery.BigQuery;

public class TimeTest {
  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PARSER;
  private static final DateTimeFormatter Z_TIMESTAMP_PARSER;
  private static final DateTimeFormatter OFFS_TIMESTAMP_PARSER;
  private static final Logger LOG = LoggerFactory.getLogger(GetSchemaTests.class);
  static {
    DateTimeFormatter dateTimePart =
        new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral(' ')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();
      DateTimeFormatter dateTimePartT =
          new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral('T')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();
    Z_TIMESTAMP_PARSER = 
      new DateTimeFormatterBuilder()
        .append(dateTimePartT)
        .appendOptional(
            new DateTimeFormatterBuilder()
              .appendLiteral('.')
              .appendFractionOfSecond(3, 7)
              .toParser())
              .appendLiteral('Z')
              .toFormatter()
              .withZoneUTC();

    OFFS_TIMESTAMP_PARSER = 
      new DateTimeFormatterBuilder()
        .append(dateTimePartT)
        .appendOptional(
            new DateTimeFormatterBuilder()
              .appendLiteral('.')
              .appendFractionOfSecond(2, 7)
              .toParser())
              .appendTimeZoneOffset(null, true, 2, 2)
              .toFormatter()
              .withZoneUTC();

    BIGQUERY_TIMESTAMP_PARSER =
        new DateTimeFormatterBuilder()
            .append(dateTimePart)
            .appendOptional(
                new DateTimeFormatterBuilder()
                    .appendLiteral('.')
                    .appendFractionOfSecond(3, 3)
                    .toParser())
            .appendLiteral(" UTC")
            .toFormatter()
            .withZoneUTC();
  }

    public static void main(String[] args) {
      LOG.info(OFFS_TIMESTAMP_PARSER.parseDateTime("2023-02-19T00:00:00+00:00").toDateTime(DateTimeZone.UTC).toString());

    }
}
