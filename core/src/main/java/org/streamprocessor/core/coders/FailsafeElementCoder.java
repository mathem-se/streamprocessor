package org.streamprocessor.core.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamprocessor.core.values.FailsafeElement;

/**
 * The {@link FailsafeElementCoder} encodes and decodes {@link FailsafeElement} objects.
 *
 * @param <OriginalT> The type of the original element to be encoded.
 * @param <CurrentT> The type of the current element to be encoded.
 */
public class FailsafeElementCoder<OriginalT, CurrentT>
        extends CustomCoder<FailsafeElement<OriginalT, CurrentT>> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomCoder.class);

    private final Coder<OriginalT> originalElementCoder;

    private final NullableCoder<CurrentT> currentElementCoder;

    private static final NullableCoder<String> STRING_CODER =
            NullableCoder.of(StringUtf8Coder.of());

    private FailsafeElementCoder(
            Coder<OriginalT> originalElementCoder, NullableCoder<CurrentT> currentElementCoder) {
        this.originalElementCoder = originalElementCoder;
        this.currentElementCoder = currentElementCoder;
    }

    public static <OriginalT, CurrentT> FailsafeElementCoder<OriginalT, CurrentT> of(
            Coder<OriginalT> originalElementCoder, NullableCoder<CurrentT> currentElementCoder) {
        return new FailsafeElementCoder<>(originalElementCoder, currentElementCoder);
    }

    static final long serialVersionUID = 8767646534L;

    /**
     * @param value
     * @param outStream
     * @exception IOException
     */
    @Override
    public void encode(FailsafeElement<OriginalT, CurrentT> value, OutputStream outStream)
            throws IOException {
        if (value == null) {
            throw new CoderException("The FailsafeElementCoder cannot encode a null object!");
        }

        try {
            originalElementCoder.encode(value.getOriginalElement(), outStream);
            currentElementCoder.encode(value.getCurrentElement(), outStream);
            STRING_CODER.encode(value.getPipelineStep(), outStream);
            STRING_CODER.encode(value.getExceptionType(), outStream);
            STRING_CODER.encode(value.getExceptionDetails(), outStream);
            STRING_CODER.encode(value.getEventTimestamp(), outStream);

        } catch (IOException e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "FailsafeElement.encode()",
                    e.toString());
            throw (e);
        }
    }

    /**
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
            String exceptionDetails = STRING_CODER.decode(inStream);
            String eventTimestamp = STRING_CODER.decode(inStream);

            return FailsafeElement.of(originalElement, currentElement)
                    .setPipelineStep(pipelineStep)
                    .setExceptionType(exception)
                    .setExceptionDetails(exceptionDetails)
                    .setEventTimestamp(eventTimestamp);
        } catch (IOException e) {
            LOG.error(
                    "exception[{}] step[{}] details[{}]",
                    e.getClass().getName(),
                    "FailsafeElement.decode()",
                    e.toString());
            throw (e);
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public TypeDescriptor<FailsafeElement<OriginalT, CurrentT>> getEncodedTypeDescriptor() {
        return new TypeDescriptor<FailsafeElement<OriginalT, CurrentT>>() {}.where(
                        new TypeParameter<OriginalT>() {},
                        originalElementCoder.getEncodedTypeDescriptor())
                .where(
                        new TypeParameter<CurrentT>() {},
                        currentElementCoder.getEncodedTypeDescriptor());
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
}
