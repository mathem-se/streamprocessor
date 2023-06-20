package org.streamprocessor.core.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.values.FailsafeElement;

public class FailSafeElementToPubsubMessageFn<T>
        extends DoFn<FailsafeElement<PubsubMessage, T>, PubsubMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(FailSafeElementToPubsubMessageFn.class);

    public FailSafeElementToPubsubMessageFn() {}
    ;

    @ProcessElement
    public void processElement(
            @Element FailsafeElement<PubsubMessage, T> received, OutputReceiver<PubsubMessage> out)
            throws Exception {
        try {
            out.output(received.getDeadletterPubsubMessage());
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "FailSafeElementToPubsubMessageFn.processElement()",
                    e.toString());
            throw e;
        }
    }
}
