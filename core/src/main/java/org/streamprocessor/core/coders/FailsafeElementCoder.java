package org.streamprocessor.core.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.helpers.FailsafeElement;

// TODO: Testing the FailsafeCoder raw class, check if type is needed.
public class FailsafeElementCoder extends CustomCoder<FailsafeElement<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomCoder.class);

    public static FailsafeElementCoder of() {
        return new FailsafeElementCoder();
    }

    public FailsafeElementCoder() {}

    static final long serialVersionUID = 8767646534L;

    /**
     * Serialize Row using a schema hash reference and a RowCoder. The RowCoder is cached with the
     * schema hash as key to avoid calls to Data catalog for each Row.
     *
     * @param value
     * @param outStream
     * @return Nothing
     * @exception IOException
     */
    @Override
    public void encode(FailsafeElement<Row> value, OutputStream outStream) throws IOException {
        try {
            SerializableCoder.of(FailsafeElement.class).encode(value, outStream);
        } catch (IOException e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "FailsafeCoder.encode()",
                    e.toString());
            throw (e);
        }
    }

    // TODO: update comment.
    /**
     * Look up the RowCoder to use given the schema hash, if none use the linkedResource ref. Decode
     * the stream to get the Row.
     *
     * @param inStream
     * @return FailsafeElement
     * @exception IOException
     */
    @Override
    public FailsafeElement decode(InputStream inStream) throws IOException {
        try {
            return SerializableCoder.of(FailsafeElement.class).decode(inStream);
        } catch (IOException e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "FailsafeCoder.decode()",
                    e.toString());
            throw (e);
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
}
