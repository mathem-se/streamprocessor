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

package org.streamprocessor.core.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubJsonClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * A class to fan out pubsub messages to different topics according to their entity attribute.
 *
 * @author Robert Sahlin
 */
public class PublisherFn extends DoFn<KV<String, Iterable<PubsubMessage>>, Integer> {

    static final long serialVersionUID = 234L;
    private static final int MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT = ((10 * 1000 * 1000) / 4) * 3;
    private static final int MAX_PUBLISH_BATCH_SIZE = 100;

    // Counters
    private final Counter batchCounter = Metrics.counter(PublisherFn.class, "batches");
    private final Counter elementCounter = SinkMetrics.elementsWritten();
    private final Counter byteCounter = SinkMetrics.bytesWritten();

    private transient PubsubClient pubsubClient;
    private int maxPublishBatchByteSize;
    private int maxPublishBatchSize;
    private String projectId;

    public PublisherFn(int maxPublishBatchSize, int maxPublishBatchByteSize) {
        this.maxPublishBatchSize = maxPublishBatchSize;
        this.maxPublishBatchByteSize = maxPublishBatchByteSize;
    }

    public PublisherFn() {
        this(MAX_PUBLISH_BATCH_SIZE, MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT);
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws IOException {
        checkState(pubsubClient == null, "startBundle invoked without prior finishBundle");
        this.pubsubClient =
                PubsubJsonClient.FACTORY.newClient(
                        null, null, c.getPipelineOptions().as(PubsubOptions.class));
        this.projectId = c.getPipelineOptions().as(GcpOptions.class).getProject();
    }

    /** BLOCKING Send {@code messages} as a batch to Pubsub. */
    private void publishBatch(String topic, List<OutgoingMessage> messages, int bytes)
            throws IOException {
        TopicPath topicPath = PubsubClient.topicPathFromName(projectId, topic);
        int n = pubsubClient.publish(topicPath, messages);
        checkState(
                n == messages.size(),
                "Attempted to publish %s messages but %s were successful",
                messages.size(),
                n);
        batchCounter.inc();
        elementCounter.inc(messages.size());
        byteCounter.inc(bytes);
    }

    @ProcessElement
    public void processElement(
            ProcessContext c,
            @Element KV<String, Iterable<PubsubMessage>> kv,
            OutputReceiver<Integer> out)
            throws Exception {
        Iterator<PubsubMessage> itr = kv.getValue().iterator();

        List<OutgoingMessage> pubsubMessages = new ArrayList<>(maxPublishBatchSize);
        int bytes = 0;
        while (itr.hasNext()) {
            PubsubMessage pubsubMessage = itr.next();
            long epoch = Long.parseLong(pubsubMessage.getAttribute("event_timestamp"));
            OutgoingMessage message =
                    OutgoingMessage.of(
                            pubsubMessage, epoch, pubsubMessage.getAttribute("event_uuid"));
            if ((!pubsubMessages.isEmpty()
                            && bytes + message.message().getData().size() > maxPublishBatchByteSize)
                    || pubsubMessages.size() >= maxPublishBatchSize) {
                publishBatch(kv.getKey(), pubsubMessages, bytes);
                pubsubMessages.clear();
                bytes = 0;
            }
            pubsubMessages.add(message);
            bytes += message.message().getData().size();
        }
        if (!pubsubMessages.isEmpty()) {
            // BLOCKS until published.
            publishBatch(kv.getKey(), pubsubMessages, bytes);
        }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
        pubsubClient.close();
        pubsubClient = null;
    }
}
