package org.streamprocessor.core.io.bigquery;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum FailureFields {
    PIPELINE_STEP("pipeline_step"),
    ORIGINAL_ATTRIBUTE("original_attribute"),
    ORIGINAL_PAYLOAD("original_payload"),
    NEW_ATTRIBUTE("new_attribute"),
    NEW_PAYLOAD("new_payload"),
    EXCEPTION_TYPE("exception_type"),
    STACK_TRACE("stack_trace"),
    METADATA_TIMESTAMP("_metadata_timestamp");

    private final String value;
}
