package org.streamprocessor.core.helpers;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.hamcrest.core.IsInstanceOf;
import org.json.JSONObject;
import org.streamprocessor.core.io.bigquery.FailureFields;

@Getter
public class FailsafeElement<OriginalT, CurrentT> implements Serializable {
    private final OriginalT originalElement;
    @Nullable private final CurrentT currentElement;
    private String pipelineStep;
    private String exceptionType;
    private Throwable exceptionDetails;
    private String eventTimestamp;

    public FailsafeElement(OriginalT originalElement, @Nullable CurrentT currentElement) {
        this.originalElement = originalElement;
        this.currentElement = currentElement;
    }

    public static <OriginalT, CurrentT> FailsafeElement<OriginalT, CurrentT> of(
            OriginalT originalElement, CurrentT currentElement) {
        return new FailsafeElement<>(originalElement, currentElement);
    }

    public FailsafeElement<OriginalT, CurrentT> setPipelineStep(String pipelineStep) {
        this.pipelineStep = pipelineStep;
        return this;
    }

    public FailsafeElement<OriginalT, CurrentT> setExceptionType(String exception) {
        this.exceptionType = exception;
        return this;
    }

    public FailsafeElement<OriginalT, CurrentT> setExceptionDetails(Throwable exceptionDetails) {
        this.exceptionDetails = exceptionDetails;
        return this;
    }

    public FailsafeElement<OriginalT, CurrentT> setEventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
        return this;
    }

    // TODO: WIP
    public <U extends PubsubMessage> PubsubMessage getPubsubMessage() {

        JSONObject pubsubData = new JSONObject(((PubsubMessage) originalElement).getPayload());
        JSONObject pubsubAttributes =  new JSONObject(((PubsubMessage) originalElement).getAttributeMap());

        JSONObject payload = new JSONObject()
                        .put(FailureFields.ORIGINAL_ATTRIBUTE.getValue(), pubsubAttributes)
                        .put(FailureFields.ORIGINAL_PAYLOAD.getValue(), pubsubData)
                        .put(FailureFields.PIPELINE_STEP.getValue(), pipelineStep)
                        .put(FailureFields.EXCEPTION_TYPE.getValue(), exceptionType)
                        .put(FailureFields.EXCEPTION_DETAILS.getValue(), exceptionDetails)
                        .put(FailureFields.METADATA_TIMESTAMP.getValue(), eventTimestamp);
        return null;
    }
}
