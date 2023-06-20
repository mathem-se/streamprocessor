package org.streamprocessor.core.values;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.streamprocessor.core.io.bigquery.FailureFields;

@Getter
public class FailsafeElement<OriginalT, CurrentT> implements Serializable {
    private final OriginalT originalElement;
    @Nullable private final CurrentT currentElement;
    private String pipelineStep;
    private String exceptionType;
    private String exceptionDetails;
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

    public FailsafeElement<OriginalT, CurrentT> setExceptionDetails(String exceptionDetails) {
        this.exceptionDetails = exceptionDetails;
        return this;
    }

    public FailsafeElement<OriginalT, CurrentT> setEventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
        return this;
    }

    /**
     * @param deadletterEntity Data contract entity for deadletter contract.
     * @return PubsubMessage with the original payload and attributes, and the error details.
     */
    public PubsubMessage getDeadletterPubsubMessage(String deadletterEntity) {

        if (!(originalElement instanceof PubsubMessage)) {
            throw new IllegalArgumentException("Original element is not of type PubsubMessage.");
        }

        Map<String, String> attributes =
                new HashMap<>() {
                    {
                        put("timestamp", Instant.now().toString());
                        put("event_timestamp", eventTimestamp);
                        put("entity", deadletterEntity);
                    }
                };

        JSONObject pubsubAttributes =
                new JSONObject(((PubsubMessage) originalElement).getAttributeMap());
        JSONObject pubsubData = new JSONObject(((PubsubMessage) originalElement).getPayload());

        JSONObject payload =
                new JSONObject()
                        .put(FailureFields.ORIGINAL_ATTRIBUTE.getValue(), pubsubAttributes)
                        .put(FailureFields.ORIGINAL_PAYLOAD.getValue(), pubsubData)
                        .put(FailureFields.PIPELINE_STEP.getValue(), pipelineStep)
                        .put(FailureFields.EXCEPTION_TYPE.getValue(), exceptionType)
                        .put(FailureFields.EXCEPTION_DETAILS.getValue(), exceptionDetails)
                        .put(FailureFields.METADATA_TIMESTAMP.getValue(), eventTimestamp);

        return new PubsubMessage(payload.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
