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

import com.google.api.services.bigquery.model.TableRow;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BqUtils}. */
@RunWith(JUnit4.class)
public class BqUtilsTest {
    private static final Schema FLAT_TYPE =
            Schema.builder()
                    .addNullableField("id", Schema.FieldType.INT64)
                    .addNullableField("value", Schema.FieldType.DOUBLE)
                    .addNullableField("name", Schema.FieldType.STRING)
                    .addNullableField("timestamp_variant1", Schema.FieldType.DATETIME)
                    .addNullableField("timestamp_variant2", Schema.FieldType.DATETIME)
                    .addNullableField("timestamp_variant3", Schema.FieldType.DATETIME)
                    .addNullableField("timestamp_variant4", Schema.FieldType.DATETIME)
                    .addNullableField("timestamp_variant5", Schema.FieldType.DATETIME)
                    .addNullableField("timestamp_variant6", Schema.FieldType.DATETIME)
                    .addNullableField("datetime", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                    .addNullableField(
                            "datetime0ms", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                    .addNullableField(
                            "datetime0s_ns", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                    .addNullableField(
                            "datetime0s_0ns", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                    .addNullableField("date", Schema.FieldType.logicalType(SqlTypes.DATE))
                    .addNullableField("time", Schema.FieldType.logicalType(SqlTypes.TIME))
                    .addNullableField("time0ms", Schema.FieldType.logicalType(SqlTypes.TIME))
                    .addNullableField("time0s_ns", Schema.FieldType.logicalType(SqlTypes.TIME))
                    .addNullableField("time0s_0ns", Schema.FieldType.logicalType(SqlTypes.TIME))
                    .addNullableField("valid", Schema.FieldType.BOOLEAN)
                    .addNullableField("binary", Schema.FieldType.BYTES)
                    .addNullableField("numeric", Schema.FieldType.DECIMAL)
                    .addNullableField("boolean", Schema.FieldType.BOOLEAN)
                    .addNullableField("long", Schema.FieldType.INT64)
                    .addNullableField("double", Schema.FieldType.DOUBLE)
                    .build();

    private static final Schema FLAT_WRONG_TYPE =
            Schema.builder()
                    .addNullableField("id", Schema.FieldType.INT64)
                    .addNullableField("value", Schema.FieldType.DOUBLE)
                    .build();

    // Make sure that chosen BYTES test value is the same after a full base64 round trip.
    private static final Row FLAT_ROW =
            Row.withSchema(FLAT_TYPE)
                    .addValues(
                            123L,
                            123.456,
                            "test",
                            ISODateTimeFormat.dateHourMinuteSecondFraction()
                                    .withZoneUTC()
                                    .parseDateTime("2019-08-16T13:52:07.000"),
                            ISODateTimeFormat.dateHourMinuteSecondFraction()
                                    .withZoneUTC()
                                    .parseDateTime("2019-08-17T14:52:07.123"),
                            ISODateTimeFormat.dateHourMinuteSecondFraction()
                                    .withZoneUTC()
                                    .parseDateTime("2019-08-18T15:52:07.123"),
                            new DateTime(123456),
                            ISODateTimeFormat.dateTimeNoMillis()
                                    .withZoneUTC()
                                    .parseDateTime("2019-08-19T16:52:07Z"),
                            ISODateTimeFormat.dateHourMinuteSecondFraction()
                                    .withZoneUTC()
                                    .parseDateTime("2023-03-06T15:29:39.766"),
                            LocalDateTime.parse("2020-11-02T12:34:56.789876"),
                            LocalDateTime.parse("2020-11-02T12:34:56"),
                            LocalDateTime.parse("2020-11-02T12:34:00.789876"),
                            LocalDateTime.parse("2020-11-02T12:34"),
                            LocalDate.parse("2020-11-02"),
                            LocalTime.parse("12:34:56.789876"),
                            LocalTime.parse("12:34:56"),
                            LocalTime.parse("12:34:00.789876"),
                            LocalTime.parse("12:34"),
                            false,
                            Base64.getDecoder().decode("ABCD1234"),
                            new BigDecimal("123.456").setScale(3, RoundingMode.HALF_UP),
                            true,
                            123L,
                            123.456d)
                    .build();

    private static final JSONObject JSON_FLAT_ROW =
            new JSONObject()
                    .put("id", "123")
                    .put("value", "123.456")
                    .put("name", "test")
                    .put("timestamp_variant1", "2019-08-16 13:52:07 UTC")
                    .put("timestamp_variant2", "2019-08-17 14:52:07.123 UTC")
                    // we'll loose precession, but it's something BigQuery can output!
                    .put("timestamp_variant3", "2019-08-18 15:52:07.123456 UTC")
                    .put(
                            "timestamp_variant4",
                            String.valueOf(
                                    new DateTime(123456L, ISOChronology.getInstanceUTC())
                                                    .getMillis()
                                            / 1000.0D))
                    .put("timestamp_variant5", "2019-08-19 16:52:07Z")
                    .put("timestamp_variant6", "2023-03-06 15:29:39.766632+00:00")
                    .put("datetime", "2020-11-02T12:34:56.789876")
                    .put("datetime0ms", "2020-11-02T12:34:56")
                    .put("datetime0s_ns", "2020-11-02T12:34:00.789876")
                    .put("datetime0s_0ns", "2020-11-02T12:34:00")
                    .put("date", "2020-11-02")
                    .put("time", "12:34:56.789876")
                    .put("time0ms", "12:34:56")
                    .put("time0s_ns", "12:34:00.789876")
                    .put("time0s_0ns", "12:34:00")
                    .put("valid", "false")
                    .put("binary", "ABCD1234")
                    .put("numeric", "123.456")
                    .put("boolean", true)
                    .put("long", 123L)
                    .put("double", 123.456d);

    private static final JSONObject JSON_FLAT_WRONG_TYPE_ROW =
            new JSONObject().put("id", "hej").put("value", "123.456");

    private static final Schema ENUM_TYPE =
            Schema.builder()
                    .addNullableField(
                            "color",
                            Schema.FieldType.logicalType(
                                    EnumerationType.create("RED", "GREEN", "BLUE")))
                    .build();

    private static final Schema ENUM_STRING_TYPE =
            Schema.builder().addNullableField("color", Schema.FieldType.STRING).build();

    private static final Schema ARRAY_TYPE =
            Schema.builder().addArrayField("ids", Schema.FieldType.INT64).build();

    private static final Schema ROW_TYPE =
            Schema.builder().addNullableField("row", Schema.FieldType.row(FLAT_TYPE)).build();

    private static final Schema MAP_TYPE =
            Schema.builder()
                    .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.DOUBLE)
                    .build();

    private static final Schema MAP_TYPE_NULL_VALUE =
            Schema.builder()
                    .addMapField(
                            "map",
                            Schema.FieldType.STRING,
                            Schema.FieldType.STRING.withNullable(true))
                    .build();

    private static final TableRow BQ_FLAT_ROW =
            BqUtils.convertJsonToTableRow("foo", JSON_FLAT_ROW.toString());

    private static final TableRow BQ_ROW_ROW = new TableRow().set("row", BQ_FLAT_ROW);

    private static final Row NULL_FLAT_ROW =
            Row.withSchema(FLAT_TYPE)
                    .addValues(
                            null, null, null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null, null, null, null, null, null, null, null, null)
                    .build();

    private static final TableRow BQ_NULL_FLAT_ROW =
            new TableRow()
                    .set("id", null)
                    .set("value", null)
                    .set("name", null)
                    .set("timestamp_variant1", null)
                    .set("timestamp_variant2", null)
                    .set("timestamp_variant3", null)
                    .set("timestamp_variant4", null)
                    .set("timestamp_variant5", null)
                    .set("timestamp_variant6", null)
                    .set("datetime", null)
                    .set("datetime0ms", null)
                    .set("datetime0s_ns", null)
                    .set("datetime0s_0ns", null)
                    .set("date", null)
                    .set("time", null)
                    .set("time0ms", null)
                    .set("time0s_ns", null)
                    .set("time0s_0ns", null)
                    .set("valid", null)
                    .set("binary", null)
                    .set("numeric", null)
                    .set("boolean", null)
                    .set("long", null)
                    .set("double", null);

    private static final Row ENUM_ROW =
            Row.withSchema(ENUM_TYPE).addValues(new EnumerationType.Value(1)).build();

    private static final Row ENUM_STRING_ROW =
            Row.withSchema(ENUM_STRING_TYPE).addValues("GREEN").build();

    private static final TableRow BQ_ENUM_ROW = new TableRow().set("color", "GREEN");

    private static final Row ARRAY_ROW =
            Row.withSchema(ARRAY_TYPE).addValues((Object) Arrays.asList(123L, 124L)).build();

    private static final Row MAP_ROW =
            Row.withSchema(MAP_TYPE).addValues(ImmutableMap.of("test", 123.456)).build();

    private static final Row ROW_ROW = Row.withSchema(ROW_TYPE).addValues(FLAT_ROW).build();

    private static final JSONObject ARRAY_JSON =
            new JSONObject().put("ids", new JSONArray().put(123L).put(124L));
    private static final TableRow BQ_ARRAY_ROW =
            BqUtils.convertJsonToTableRow("foo", ARRAY_JSON.toString());

    private static final JSONObject MAP_JSON =
            new JSONObject().put("map", new JSONObject().put("test", 123.456));

    private static final JSONObject MAP_JSON_NULL_VALUE =
            new JSONObject().put("map", new JSONObject().put("test", JSONObject.NULL));

    private static final TableRow BQ_MAP_ROW =
            BqUtils.convertJsonToTableRow("foo", MAP_JSON.toString());

    private static final TableRow BQ_MAP_ROW_NULL_VALUE =
            BqUtils.convertJsonToTableRow("foo", MAP_JSON_NULL_VALUE.toString());

    @Test
    public void testToBeamRow_flat() {
        Row beamRow = BqUtils.toBeamRow("foo", FLAT_TYPE, BQ_FLAT_ROW);
        assertEquals(FLAT_ROW, beamRow);
    }

    @Test
    public void testToBeamValue() {
        FieldType fieldType = Schema.FieldType.DATETIME;
        Object jsonBQValueWinterTime = "2020-11-02 12:34:56";
        Object expectedWinterTime = DateTime.parse("2020-11-02T11:34:56Z");
        Object parsedWinterTime = BqUtils.toBeamValue("entity", fieldType, jsonBQValueWinterTime);
        assertEquals(expectedWinterTime, parsedWinterTime);
        Object jsonBQValue = "2020-08-02 12:34:56";
        Object expected = DateTime.parse("2020-08-02T10:34:56Z");
        Object actual = BqUtils.toBeamValue("entity", fieldType, jsonBQValue);
        assertEquals(expected, actual);
    }

    @Test
    public void testToBeamRow_null() {
        Row beamRow = BqUtils.toBeamRow("foo", FLAT_TYPE, BQ_NULL_FLAT_ROW);
        assertEquals(NULL_FLAT_ROW, beamRow);
    }

    @Test
    public void testToBeamRow_enum() {
        Row beamRow = BqUtils.toBeamRow("foo", ENUM_STRING_TYPE, BQ_ENUM_ROW);
        assertEquals(ENUM_STRING_ROW, beamRow);
    }

    @Test
    public void testToBeamRow_row() {
        Row beamRow = BqUtils.toBeamRow("foo", ROW_TYPE, BQ_ROW_ROW);
        assertEquals(ROW_ROW, beamRow);
    }

    @Test
    public void testToBeamRow_array() {
        Row beamRow = BqUtils.toBeamRow("foo", ARRAY_TYPE, BQ_ARRAY_ROW);
        assertEquals(ARRAY_ROW, beamRow);
    }

    @Test
    public void testToTableRow_map() {
        Row beamRow = BqUtils.toBeamRow("foo", MAP_TYPE, BQ_MAP_ROW);
        assertEquals(MAP_ROW, beamRow);
    }

    @Test
    public void testToTableRow_mapNullValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("test", null);
        Row mapNullValue = Row.withSchema(MAP_TYPE_NULL_VALUE).addValues(map).build();
        Row beamRow = BqUtils.toBeamRow("foo", MAP_TYPE_NULL_VALUE, BQ_MAP_ROW_NULL_VALUE);
        assertEquals(mapNullValue, beamRow);
    }
}
