package org.streamprocessor.core.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.streamprocessor.core.helpers.FailsafeElement;

public class ExtractRowFromFailsafeElement extends DoFn<FailsafeElement<Row>, Row> {

    @ProcessElement
    public void processElement(@Element FailsafeElement<Row> received, OutputReceiver<Row> out) {
        out.output(received.getNewElement());
    }
}
