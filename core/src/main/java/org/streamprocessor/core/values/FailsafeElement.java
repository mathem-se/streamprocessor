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
     * @return PubsubMessage with the original payload and attributes, and the error details.
     */
    public PubsubMessage getDeadletterPubsubMessage() {

        if (!(originalElement instanceof PubsubMessage)) {
            throw new IllegalArgumentException("Original element is not of type PubsubMessage.");
        }

        Map<String, String> originalAttributes =
                ((PubsubMessage) originalElement).getAttributeMap();

        String originalPayload =
                new String(((PubsubMessage) originalElement).getPayload(), StandardCharsets.UTF_8);

        String originalEntity = originalAttributes.get("entity");

        /*
         * TODO: Decide what should be included in the deadletter attributes.
         * Setting entity of the failed message as original entity for now.
         */
        Map<String, String> attributes =
                new HashMap<>() {
                    {
                        put("timestamp", Instant.now().toString());
                        put("event_timestamp", eventTimestamp);
                        put("original_entity", originalEntity);
                    }
                };

        JSONObject payload =
                new JSONObject()
                        .put(FailureFields.ORIGINAL_ATTRIBUTE.getValue(), originalAttributes)
                        .put(FailureFields.ORIGINAL_PAYLOAD.getValue(), originalPayload)
                        .put(FailureFields.PIPELINE_STEP.getValue(), pipelineStep)
                        .put(FailureFields.EXCEPTION_TYPE.getValue(), exceptionType)
                        .put(FailureFields.EXCEPTION_DETAILS.getValue(), exceptionDetails)
                        .put(FailureFields.EVENT_TIMESTAMP.getValue(), eventTimestamp);

        return new PubsubMessage(payload.toString().getBytes(StandardCharsets.UTF_8), attributes);
    }
}
