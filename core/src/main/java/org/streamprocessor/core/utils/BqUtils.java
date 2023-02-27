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
package org.streamprocessor.core.utils;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for BigQuery related operations. */
@SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20506)
})
public class BqUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BqUtils.class);

    enum StandardSQLTypeName {
        /** A Boolean value (true or false). */
        BOOL,
        /** A 64-bit signed integer value. */
        INT64,
        /** A 64-bit IEEE binary floating-point value. */
        FLOAT64,
        /** A decimal value with 38 digits of precision and 9 digits of scale. */
        NUMERIC,
        /** Variable-length character (Unicode) data. */
        STRING,
        /** Variable-length binary data. */
        BYTES,
        /** Container of ordered fields each with a type (required) and field name (optional). */
        STRUCT,
        /** Ordered list of zero or more elements of any non-array type. */
        ARRAY,
        /**
         * Represents an absolute point in time, with microsecond precision. Values range between
         * the years 1 and 9999, inclusive.
         */
        TIMESTAMP,
        /**
         * Represents a logical calendar date. Values range between the years 1 and 9999, inclusive.
         */
        DATE,
        /** Represents a time, independent of a specific date, to microsecond precision. */
        TIME,
        /**
         * Represents a year, month, day, hour, minute, second, and subsecond (microsecond
         * precision).
         */
        DATETIME,
        /** Represents a collection of points, lines, and polygons. Represented as a point set. */
        GEOGRAPHY
    }

    enum Mode {
        NULLABLE,
        REQUIRED,
        REPEATED
    }

    private static final String BIGQUERY_TIME_PATTERN = "HH:mm:ss[.SSSSSS]";
    private static final java.time.format.DateTimeFormatter BIGQUERY_TIME_FORMATTER =
            java.time.format.DateTimeFormatter.ofPattern(BIGQUERY_TIME_PATTERN);
    private static final java.time.format.DateTimeFormatter BIGQUERY_DATETIME_FORMATTER =
            java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd'T'" + BIGQUERY_TIME_PATTERN);

    private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER;

    /**
     * Native BigQuery formatter for it's timestamp format, depending on the milliseconds stored in
     * the column, the milli second part will be 6, 3 or absent. Example {@code 2019-08-16
     * 00:52:07[.123]|[.123456] UTC}
     */
    private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PARSER;

    private static final DateTimeFormatter Z_TIMESTAMP_PARSER;
    private static final DateTimeFormatter OFFS_TIMESTAMP_PARSER;
    private static final DateTimeFormatter DATETIME_PARSER;

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

        DATETIME_PARSER =
                new DateTimeFormatterBuilder()
                        .append(dateTimePartT)
                        .appendOptional(
                                new DateTimeFormatterBuilder()
                                        .appendLiteral('.')
                                        .appendFractionOfSecond(2, 7)
                                        .toParser())
                        .toFormatter()
                        .withZoneUTC();

        BIGQUERY_TIMESTAMP_PARSER =
                new DateTimeFormatterBuilder()
                        .append(dateTimePart)
                        .appendOptional(
                                new DateTimeFormatterBuilder()
                                        .appendLiteral('.')
                                        .appendFractionOfSecond(3, 6)
                                        .toParser())
                        .appendLiteral(" UTC")
                        .toFormatter()
                        .withZoneUTC();
        BIGQUERY_TIMESTAMP_PRINTER =
                new DateTimeFormatterBuilder()
                        .append(dateTimePart)
                        .appendLiteral('.')
                        .appendFractionOfSecond(6, 6)
                        .appendLiteral(" UTC")
                        .toFormatter();
    }

    private static final Map<TypeName, StandardSQLTypeName> BEAM_TO_BIGQUERY_TYPE_MAPPING =
            ImmutableMap.<TypeName, StandardSQLTypeName>builder()
                    .put(TypeName.BYTE, StandardSQLTypeName.INT64)
                    .put(TypeName.INT16, StandardSQLTypeName.INT64)
                    .put(TypeName.INT32, StandardSQLTypeName.INT64)
                    .put(TypeName.INT64, StandardSQLTypeName.INT64)
                    .put(TypeName.FLOAT, StandardSQLTypeName.FLOAT64)
                    .put(TypeName.DOUBLE, StandardSQLTypeName.FLOAT64)
                    .put(TypeName.DECIMAL, StandardSQLTypeName.NUMERIC)
                    .put(TypeName.BOOLEAN, StandardSQLTypeName.BOOL)
                    .put(TypeName.ARRAY, StandardSQLTypeName.ARRAY)
                    .put(TypeName.ITERABLE, StandardSQLTypeName.ARRAY)
                    .put(TypeName.ROW, StandardSQLTypeName.STRUCT)
                    .put(TypeName.DATETIME, StandardSQLTypeName.TIMESTAMP)
                    .put(TypeName.STRING, StandardSQLTypeName.STRING)
                    .put(TypeName.BYTES, StandardSQLTypeName.BYTES)
                    .build();

    private static final Map<TypeName, Function<String, @Nullable Object>> JSON_VALUE_PARSERS =
            ImmutableMap.<TypeName, Function<String, @Nullable Object>>builder()
                    .put(TypeName.BYTE, Byte::valueOf)
                    .put(TypeName.INT16, Short::valueOf)
                    .put(TypeName.INT32, Integer::valueOf)
                    .put(TypeName.INT64, Long::valueOf)
                    .put(TypeName.FLOAT, Float::valueOf)
                    .put(TypeName.DOUBLE, Double::valueOf)
                    .put(TypeName.DECIMAL, BigDecimal::new)
                    .put(TypeName.BOOLEAN, Boolean::valueOf)
                    .put(TypeName.STRING, str -> str)
                    .put(
                            TypeName.DATETIME,
                            str -> {
                                if (str == null || str.length() == 0) {
                                    return null;
                                }
                                if (str.endsWith("UTC")) {
                                    return BIGQUERY_TIMESTAMP_PARSER
                                            .parseDateTime(str)
                                            .toDateTime(DateTimeZone.UTC);
                                } else if (str.endsWith("Z")) {
                                    return Z_TIMESTAMP_PARSER
                                            .parseDateTime(str)
                                            .toDateTime(DateTimeZone.UTC);
                                } else if (str.contains("T") && str.contains("+")) {
                                    return OFFS_TIMESTAMP_PARSER
                                            .parseDateTime(str)
                                            .toDateTime(DateTimeZone.UTC);
                                } else if (str.contains("T")) {
                                    return DATETIME_PARSER
                                            .parseDateTime(str)
                                            .toDateTime(DateTimeZone.UTC);
                                } else {
                                    return new DateTime(
                                            (long) (Double.parseDouble(str) * 1000),
                                            ISOChronology.getInstanceUTC());
                                }
                            })
                    .put(TypeName.BYTES, str -> BaseEncoding.base64().decode(str))
                    .build();

    // TODO: BigQuery code should not be relying on Calcite metadata fields. If so, this belongs
    // in the SQL package.
    static final Map<String, StandardSQLTypeName> BEAM_TO_BIGQUERY_LOGICAL_MAPPING =
            ImmutableMap.<String, StandardSQLTypeName>builder()
                    .put(SqlTypes.DATE.getIdentifier(), StandardSQLTypeName.DATE)
                    .put(SqlTypes.TIME.getIdentifier(), StandardSQLTypeName.TIME)
                    .put(SqlTypes.DATETIME.getIdentifier(), StandardSQLTypeName.DATETIME)
                    .put("SqlTimeWithLocalTzType", StandardSQLTypeName.TIME)
                    .put("SqlCharType", StandardSQLTypeName.STRING)
                    .put("Enum", StandardSQLTypeName.STRING)
                    .build();

    private static final String BIGQUERY_MAP_KEY_FIELD_NAME = "key";
    private static final String BIGQUERY_MAP_VALUE_FIELD_NAME = "value";

    /**
     * Get the corresponding BigQuery {@link StandardSQLTypeName} for supported Beam {@link
     * FieldType}.
     */
    static StandardSQLTypeName toStandardSQLTypeName(FieldType fieldType) {
        StandardSQLTypeName ret;
        if (fieldType.getTypeName().isLogicalType()) {
            Schema.LogicalType<?, ?> logicalType =
                    Preconditions.checkArgumentNotNull(fieldType.getLogicalType());
            ret = BEAM_TO_BIGQUERY_LOGICAL_MAPPING.get(logicalType.getIdentifier());
            if (ret == null) {
                throw new IllegalArgumentException(
                        "Cannot convert Beam logical type: "
                                + logicalType.getIdentifier()
                                + " to BigQuery type.");
            }
        } else {
            ret = BEAM_TO_BIGQUERY_TYPE_MAPPING.get(fieldType.getTypeName());
            if (ret == null) {
                throw new IllegalArgumentException(
                        "Cannot convert Beam type: "
                                + fieldType.getTypeName()
                                + " to BigQuery type.");
            }
        }
        return ret;
    }

    private static final SerializableFunction<Row, TableRow> ROW_TO_TABLE_ROW =
            new ToTableRow<>(SerializableFunctions.identity());

    /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
    public static SerializableFunction<Row, TableRow> toTableRow() {
        return ROW_TO_TABLE_ROW;
    }

    /** Convert a Beam schema type to a BigQuery {@link TableRow}. */
    public static <T> SerializableFunction<T, TableRow> toTableRow(
            SerializableFunction<T, Row> toRow) {
        return new ToTableRow<>(toRow);
    }

    /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
    private static class ToTableRow<T> implements SerializableFunction<T, TableRow> {
        private final SerializableFunction<T, Row> toRow;

        ToTableRow(SerializableFunction<T, Row> toRow) {
            this.toRow = toRow;
        }

        @Override
        public TableRow apply(T input) {
            return toTableRow(toRow.apply(input));
        }
    }

    /** Convert a BigQuery TableRow to a Beam Row. */
    public static TableRow toTableRow(Row row) {
        TableRow output = new TableRow();
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object value = row.getValue(i);
            Field schemaField = row.getSchema().getField(i);
            output = output.set(schemaField.getName(), fromBeamField(schemaField.getType(), value));
        }
        return output;
    }

    private static @Nullable Object fromBeamField(FieldType fieldType, Object fieldValue) {
        if (fieldValue == null) {
            if (!fieldType.getNullable()) {
                throw new IllegalArgumentException("Field is not nullable.");
            }
            return null;
        }

        switch (fieldType.getTypeName()) {
            case ARRAY:
            case ITERABLE:
                FieldType elementType = fieldType.getCollectionElementType();
                Iterable<?> items = (Iterable<?>) fieldValue;
                List<Object> convertedItems = Lists.newArrayListWithCapacity(Iterables.size(items));
                for (Object item : items) {
                    convertedItems.add(fromBeamField(elementType, item));
                }
                return convertedItems;

            case MAP:
                FieldType keyElementType = fieldType.getMapKeyType();
                FieldType valueElementType = fieldType.getMapValueType();
                Map<?, ?> pairs = (Map<?, ?>) fieldValue;
                convertedItems = Lists.newArrayListWithCapacity(pairs.size());
                for (Map.Entry<?, ?> pair : pairs.entrySet()) {
                    convertedItems.add(
                            new TableRow()
                                    .set(
                                            BIGQUERY_MAP_KEY_FIELD_NAME,
                                            fromBeamField(keyElementType, pair.getKey()))
                                    .set(
                                            BIGQUERY_MAP_VALUE_FIELD_NAME,
                                            fromBeamField(valueElementType, pair.getValue())));
                }
                return convertedItems;

            case ROW:
                return toTableRow((Row) fieldValue);

            case DATETIME:
                return ((Instant) fieldValue)
                        .toDateTime(DateTimeZone.UTC)
                        .toString(BIGQUERY_TIMESTAMP_PRINTER);

            case INT16:
            case INT32:
            case FLOAT:
            case BOOLEAN:
            case DOUBLE:
                // The above types have native representations in JSON for all their
                // possible values.
                return fieldValue;

            case STRING:
            case INT64:
            case DECIMAL:
                // The above types must be cast to string to be safely encoded in
                // JSON (due to JSON's float-based representation of all numbers).
                return fieldValue.toString();

            case BYTES:
                return BaseEncoding.base64().encode((byte[]) fieldValue);

            case LOGICAL_TYPE:
                // For the JSON formats of DATE/DATETIME/TIME/TIMESTAMP types that BigQuery accepts,
                // see
                // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#details_of_loading_json_data
                String identifier = fieldType.getLogicalType().getIdentifier();
                if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
                    return fieldValue.toString();
                } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
                    // LocalTime.toString() drops seconds if it is zero (see
                    // https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html#toString--).
                    // but BigQuery TIME requires seconds
                    // (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_type).
                    // Fractional seconds are optional so drop them to conserve number of bytes
                    // transferred.
                    LocalTime localTime = (LocalTime) fieldValue;
                    @SuppressWarnings(
                            "JavaLocalTimeGetNano") // Suppression is justified because seconds are
                    // always
                    // outputted.
                    java.time.format.DateTimeFormatter localTimeFormatter =
                            (0 == localTime.getNano()) ? ISO_LOCAL_TIME : BIGQUERY_TIME_FORMATTER;
                    return localTimeFormatter.format(localTime);
                } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
                    // Same rationale as SqlTypes.TIME
                    LocalDateTime localDateTime = (LocalDateTime) fieldValue;
                    @SuppressWarnings("JavaLocalDateTimeGetNano")
                    java.time.format.DateTimeFormatter localDateTimeFormatter =
                            (0 == localDateTime.getNano())
                                    ? ISO_LOCAL_DATE_TIME
                                    : BIGQUERY_DATETIME_FORMATTER;
                    return localDateTimeFormatter.format(localDateTime);
                } else if ("Enum".equals(identifier)) {
                    return fieldType
                            .getLogicalType(EnumerationType.class)
                            .toString((EnumerationType.Value) fieldValue);
                } // fall through

            default:
                return fieldValue.toString();
        }
    }

    /**
     * Tries to convert a JSON {@link TableRow} from BigQuery into a Beam {@link Row}.
     *
     * <p>Only supports basic types and arrays. Doesn't support date types or structs.
     */
    @Experimental(Kind.SCHEMAS)
    public static Row toBeamRow(Schema rowSchema, TableRow jsonBqRow) {
        // TODO deprecate toBeamRow(Schema, TableSchema, TableRow) function in favour of this
        // function.
        // This function attempts to convert TableRows without  having access to the
        // corresponding TableSchema because:
        // 1. TableSchema contains redundant information already available in the Schema object.
        // 2. TableSchema objects are not serializable and are therefore harder to propagate through
        // a
        // pipeline.
        return rowSchema.getFields().stream()
                .map(field -> toBeamRowFieldValue(field, jsonBqRow.get(field.getName())))
                .collect(toRow(rowSchema));
    }

    private static Object toBeamRowFieldValue(Field field, Object bqValue) {

        if (bqValue == null) {
            if (field.getType().getNullable()) {
                return null;
            } else {
                throw new IllegalArgumentException(
                        "Received null value for non-nullable field \"" + field.getName() + "\"");
            }
        }
        try {
            return toBeamValue(field.getType(), bqValue);
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    "Could not convert field "
                            + "`"
                            + field.getName()
                            + "`"
                            + " of type "
                            + "`"
                            + field.getType()
                            + "`"
                            + " with value "
                            + bqValue);
        }
    }

    private static @Nullable Object toBeamValue(FieldType fieldType, Object jsonBQValue) {
        if (jsonBQValue instanceof String
                || jsonBQValue instanceof Number
                || jsonBQValue instanceof Boolean) {
            String jsonBQString = jsonBQValue.toString();
            if (JSON_VALUE_PARSERS.containsKey(fieldType.getTypeName())) {
                return JSON_VALUE_PARSERS.get(fieldType.getTypeName()).apply(jsonBQString);
            } else if (fieldType.isLogicalType(SqlTypes.DATETIME.getIdentifier())) {
                return LocalDateTime.parse(jsonBQString, BIGQUERY_DATETIME_FORMATTER);
            } else if (fieldType.isLogicalType(SqlTypes.DATE.getIdentifier())) {
                return LocalDate.parse(jsonBQString);
            } else if (fieldType.isLogicalType(SqlTypes.TIME.getIdentifier())) {
                return LocalTime.parse(jsonBQString);
            }
        }

        if (jsonBQValue instanceof List) {
            FieldType ft = fieldType.getCollectionElementType();
            return ((List<Object>) jsonBQValue)
                    .stream()
                            .map(v -> toBeamValue(fieldType.getCollectionElementType(), v))
                            .collect(toList());
        }

        if (jsonBQValue instanceof Map && !(fieldType.getTypeName().isMapType())) {
            TableRow tr = new TableRow();
            tr.putAll((Map<String, Object>) jsonBQValue);
            return toBeamRow(fieldType.getRowSchema(), tr);
        }

        if (jsonBQValue instanceof Map && fieldType.getTypeName().isMapType()) {
            if (fieldType.getMapValueType().getRowSchema() == null) {
                Map<String, Object> k =
                        ((Map<String, Object>) jsonBQValue)
                                .entrySet().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e -> {
                                                            return toBeamValue(
                                                                    fieldType.getMapValueType(),
                                                                    e.getValue());
                                                        }));
                return k;
            } else {
                Map<String, Object> k =
                        ((Map<String, Object>) jsonBQValue)
                                .entrySet().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e -> {
                                                            TableRow tr = new TableRow();
                                                            tr.putAll(
                                                                    (Map<String, Object>)
                                                                            e.getValue());
                                                            return toBeamRow(
                                                                    fieldType
                                                                            .getMapValueType()
                                                                            .getRowSchema(),
                                                                    tr);
                                                        }));
                return k;
            }
        }

        throw new UnsupportedOperationException(
                "Converting BigQuery type '"
                        + jsonBQValue.getClass()
                        + "' to '"
                        + fieldType
                        + "' is not supported");
    }

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
}
