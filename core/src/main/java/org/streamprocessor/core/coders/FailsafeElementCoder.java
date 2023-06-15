package org.streamprocessor.core.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.helpers.FailsafeElement;

public class FailsafeElementCoder<OriginalT, CurrentT>
        extends CustomCoder<FailsafeElement<OriginalT, CurrentT>> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomCoder.class);

    private static final NullableCoder<String> STRING_CODER =
            NullableCoder.of(StringUtf8Coder.of());

    private final Coder<OriginalT> originalElementCoder;

    private final Coder<CurrentT> currentElementCoder;

    private FailsafeElementCoder(
            Coder<OriginalT> originalElementCoder, Coder<CurrentT> currentElementCoder) {
        this.originalElementCoder = originalElementCoder;
        this.currentElementCoder = currentElementCoder;
    }

    public static <OriginalT, CurrentT> FailsafeElementCoder<OriginalT, CurrentT> of(
            Coder<OriginalT> originalElementCoder, Coder<CurrentT> currentElementCoder) {
        return new FailsafeElementCoder<>(originalElementCoder, currentElementCoder);
    }

    static final long serialVersionUID = 8767646534L;

    /**
     * Serialize Row using a schema hash reference and a RowCoder. The RowCoder is cached with the
     * schema hash as key to avoid calls to Data catalog for each Row.
     *
     * @param value
     * @param outStream
     * @exception IOException
     */
    @Override
    public void encode(FailsafeElement<OriginalT, CurrentT> value, OutputStream outStream)
            throws IOException {
        try {
            originalElementCoder.encode(value.getOriginalElement(), outStream);
            currentElementCoder.encode(value.getCurrentElement(), outStream);
            STRING_CODER.encode(value.getPipelineStep(), outStream);
            STRING_CODER.encode(value.getException(), outStream);
            SerializableCoder.of(Throwable.class).encode(value.getExceptionDetails(), outStream);

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
    public FailsafeElement<OriginalT, CurrentT> decode(InputStream inStream) throws IOException {

        try {
            OriginalT originalElement = originalElementCoder.decode(inStream);
            CurrentT currentElement = currentElementCoder.decode(inStream);
            String pipelineStep = STRING_CODER.decode(inStream);
            String exception = STRING_CODER.decode(inStream);
            Throwable exceptionDetails = SerializableCoder.of(Throwable.class).decode(inStream);

            return FailsafeElement.of(originalElement, currentElement)
                    .setPipelineStep(pipelineStep)
                    .setException(exception)
                    .setExceptionDetails(exceptionDetails);
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
