package org.streamprocessor.core.helpers;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

@Getter
public class FailsafeElement<CurrentT> implements Serializable {
    private final PubsubMessage originalElement;
    private final CurrentT currentElement;
    private String pipelineStep;
    private String exception;
    private Throwable exceptionDetails;

    public FailsafeElement(PubsubMessage originalElement, @Nullable CurrentT currentElement) {
        this.originalElement = originalElement;
        this.currentElement = currentElement;
    }

    public FailsafeElement(
            PubsubMessage originalElement,
            @Nullable CurrentT currentElement,
            @Nullable String pipelineStep,
            @Nullable String exception,
            @Nullable Throwable exceptionDetails) {
        this.originalElement = originalElement;
        this.currentElement = currentElement;
        this.pipelineStep = pipelineStep;
        this.exception = exception;
        this.exceptionDetails = exceptionDetails;
    }
}
