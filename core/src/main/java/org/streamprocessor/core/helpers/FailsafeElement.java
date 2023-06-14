package org.streamprocessor.core.helpers;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

@Getter
public class FailsafeElement<T> implements Serializable {
    private final PubsubMessage originalElement;
    private final T newElement;
    private String pipelineStep;
    private String exception;
    private Throwable exceptionDetails;

    public FailsafeElement(PubsubMessage originalElement, @Nullable T newElement) {
        this.originalElement = originalElement;
        this.newElement = newElement;
    }

    public FailsafeElement(
            PubsubMessage originalElement,
            @Nullable T newElement,
            @Nullable String pipelineStep,
            @Nullable String exception,
            @Nullable Throwable exceptionDetails) {
        this.originalElement = originalElement;
        this.newElement = newElement;
        this.pipelineStep = pipelineStep;
        this.exception = exception;
        this.exceptionDetails = exceptionDetails;
    }
}
