package org.streamprocessor.core.io.bigquery;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum FailureFields {
    PIPELINE_STEP("pipeline_step"),
    ORIGINAL_ATTRIBUTE("original_attribute"),
    ORIGINAL_PAYLOAD("original_payload"),
    EXCEPTION_TYPE("exception_type"),
    EXCEPTION_DETAILS("exception_details"),
    EVENT_TIMESTAMP("event_timestamp");

    private final String value;
}
