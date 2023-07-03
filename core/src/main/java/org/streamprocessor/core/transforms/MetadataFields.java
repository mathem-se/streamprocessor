package org.streamprocessor.core.transforms;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class MetadataFields {

    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String EVENT_ID = "event_id";
    public static final String EXTRACT_METHOD = "extract_method";
    public static final String OPERATION = "operation";

    @AllArgsConstructor
    @Getter
    public enum ExtractMethod {
        CUSTOM_EVENT("custom_event"),
        CDC("cdc");
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
