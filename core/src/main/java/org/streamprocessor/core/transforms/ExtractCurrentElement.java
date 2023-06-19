package org.streamprocessor.core.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.streamprocessor.core.helpers.FailsafeElement;

public class ExtractCurrentElement<K, V> extends DoFn<FailsafeElement<K, V>, V> {

    @ProcessElement
    public void processElement(@Element FailsafeElement<K, V> received, OutputReceiver<V> out) {
        out.output(received.getCurrentElement());
    }
}
