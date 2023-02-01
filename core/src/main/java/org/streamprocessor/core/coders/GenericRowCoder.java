/*
 * Copyright (C) 2021 Robert Sahlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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

/**
 * A generic RowCoder class to enable Dataflow job to process Rows with different schemas. It uses
 * Caffeine as cache to avoid hitting GCP Data catalog for each Row. Requires that each Row has an
 * entity option that is used to look up the schema of a linked resource.
 *
 * @author Robert Sahlin
 * @version 1.0
 * @since 2022-04-20
 */
public class GenericRowCoder extends CustomCoder<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomCoder.class);
    String projectId;
    String datasetId;

    public static GenericRowCoder of() {
        return new GenericRowCoder();
    }

    public GenericRowCoder() {}

    public GenericRowCoder(String projectId, String datasetId) {
        this.projectId = projectId;
        this.datasetId = datasetId;
    }

    static final long serialVersionUID = 8767646534L;

    /**
     * Serialize Row using a schema hash reference and a RowCoder. The RowCoder is cached with the
     * schema hash as key to avoid calls to Data catalog for each Row.
     *
     * @param row
     * @param outStream
     * @return Nothing
     * @exception IOException
     */
    @Override
    public void encode(Row row, OutputStream outStream) throws IOException {
        try {
            SerializableCoder.of(Row.class).encode(row, outStream);
        } catch (IOException e) {
            LOG.error("GenericRowCoder encode: ", e);
            throw (e);
        }
    }

    /**
     * Look up the RowCoder to use given the schema hash, if none use the linkedResource ref. Decode
     * the stream to get the Row.
     *
     * @param inStream
     * @return Row
     * @exception IOException
     */
    @Override
    public Row decode(InputStream inStream) throws IOException {
        try {
            return SerializableCoder.of(Row.class).decode(inStream);
        } catch (IOException e) {
            LOG.error("GenericRowCoder encode: ", e);
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
