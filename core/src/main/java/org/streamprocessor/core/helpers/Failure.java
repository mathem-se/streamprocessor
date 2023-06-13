package org.streamprocessor.core.helpers;

import lombok.Builder;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.io.Serializable;

@Builder
@Getter
public class Failure implements Serializable {
    private String pipelineStep;
    private PubsubMessage pubsubMessageRaw;
    private PubsubMessage pubsubMessage;
    private String exception;
    private Throwable exceptionDetails;
}
