package org.streamprocessor.core.transforms;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class MetadataFields {

    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String EVENT_ID = "event_id";
    public static final String EXTRACT_METHOD = "extract_method";
    public static final String OPERATION = "operation";
    public static final String LOG_POSITION = "log_position";
    public static final String LOG_FILE = "log_file";

    @AllArgsConstructor
    @Getter
    public enum ExtractMethod {
        GOOGLE_SHEETS_IMPORT("google_sheets_import"),
        CLOUD_STORAGE_IMPORT("cloud_storage_import"),
        CUSTOM_EVENT("custom_event"),
        CDC("cdc"),
        BACKFILL("backfill");
        private final String value;
    }

    @AllArgsConstructor
    @Getter
    public enum Operation {
        INSERT("insert"),
        MODIFY("modify"),
        REMOVE("remove");
        private final String value;
    }
}
