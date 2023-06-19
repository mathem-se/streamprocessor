package org.streamprocessor.core.helpers;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Getter;

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
}
