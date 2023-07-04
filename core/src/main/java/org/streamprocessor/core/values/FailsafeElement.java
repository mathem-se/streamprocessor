package org.streamprocessor.core.values;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.io.bigquery.FailureFields;

@Getter
public class FailsafeElement<OriginalT, CurrentT> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FailsafeElement.class);

    private final OriginalT originalElement;
    @Nullable private final CurrentT currentElement;
    private String jobName;
    private String pipelineStep;
    private String exceptionType;
    private String exceptionDetails;
    private String failureTimestamp;

    public FailsafeElement(OriginalT originalElement, @Nullable CurrentT currentElement) {
        this.originalElement = originalElement;
        this.currentElement = currentElement;
    }

    public static <OriginalT, CurrentT> FailsafeElement<OriginalT, CurrentT> of(
            OriginalT originalElement, CurrentT currentElement) {
        return new FailsafeElement<>(originalElement, currentElement);
    }

    public FailsafeElement<OriginalT, CurrentT> setJobName(String jobName) {
        this.jobName = jobName;
        return this;
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

    public FailsafeElement<OriginalT, CurrentT> setEventTimestamp(String failureTimestamp) {
        this.failureTimestamp = failureTimestamp;
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

        JSONObject payload =
                new JSONObject()
                        .put(FailureFields.ORIGINAL_PAYLOAD.getValue(), originalPayload)
                        .put(FailureFields.JOB_NAME.getValue(), jobName)
                        .put(FailureFields.PIPELINE_STEP.getValue(), pipelineStep)
                        .put(FailureFields.EXCEPTION_TYPE.getValue(), exceptionType)
                        .put(FailureFields.EXCEPTION_DETAILS.getValue(), exceptionDetails)
                        .put(FailureFields.FAILURE_TIMESTAMP.getValue(), failureTimestamp);

        LOG.info("Created dead-letter message, uuid[{}]", originalAttributes.get("uuid"));

        return new PubsubMessage(
                payload.toString().getBytes(StandardCharsets.UTF_8), originalAttributes);
    }
}
