package org.streamprocessor.core.helpers;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Getter;

@Getter
public class FailsafeElement<OriginalT, CurrentT> implements Serializable {
    private final OriginalT originalElement;
    @Nullable private final CurrentT currentElement;
    @Nullable private String pipelineStep;
    @Nullable private String exception;
    @Nullable private Throwable exceptionDetails;

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

    public FailsafeElement<OriginalT, CurrentT> setException(String exception) {
        this.exception = exception;
        return this;
    }

    public FailsafeElement<OriginalT, CurrentT> setExceptionDetails(Throwable exceptionDetails) {
        this.exceptionDetails = exceptionDetails;
        return this;
    }
}
