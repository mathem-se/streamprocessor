package org.streamprocessor.core.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.values.FailsafeElement;

public class ExtractCurrentElementFn<K, V> extends DoFn<FailsafeElement<K, V>, V> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractCurrentElementFn.class);

    @ProcessElement
    public void processElement(@Element FailsafeElement<K, V> received, OutputReceiver<V> out)
            throws Exception {
        try {
            out.output(received.getCurrentElement());
        } catch (Exception e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "ExtractCurrentElementFn.processElement()",
                    e.toString());
            throw e;
        }
    }
}
