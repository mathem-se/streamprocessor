package org.streamprocessor.core.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

public final class JsonToTableRow {

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
