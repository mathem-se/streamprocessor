package org.streamprocessor.core.helpers;

import lombok.Builder;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
@Builder
@Getter
public class FailSafePubsubMessage {
    private PubsubMessage originalPubsubMessage;
    private PubsubMessage newPubsubMessage;
}
