package org.streamprocessor.core.transforms;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class MetadataFields {

    @AllArgsConstructor
    @Getter
    public enum Event {
        EVENT_TIMESTAMP("event_timestamp"),
        EVENT_ID("event_id");
        private final String value;
    }

    @AllArgsConstructor
    @Getter
    public enum ExtractMethod {
        EXTRACT_METHOD("extract_method"),
        CUSTOM_EVENT("custom_event"),
        CDC("cdc");
        private final String value;
    }

    @AllArgsConstructor
    @Getter
    public enum Operation {
        OPERATION("operation"),
        INSERT("insert"),
        MODIFY("modify"),
        REMOVE("remove");
        private final String value;
    }
}
