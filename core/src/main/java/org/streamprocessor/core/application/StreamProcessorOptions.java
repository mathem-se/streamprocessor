package org.streamprocessor.core.application;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Provides the custom execution options passed by the executor at the command-line.
 *
 * <p>Inherits standard configuration options.
 */
public interface StreamProcessorOptions extends DataflowPipelineOptions {
    @Description("Pubsub Input Subscription")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description("Firestore Project Id, if other than the project where Dataflow job runs")
    String getFirestoreProjectId();

    void setFirestoreProjectId(String value);

    @Description("Pubsub topic for backup of tokenized data")
    String getBackupTopic();

    void setBackupTopic(String value);

    @Description("Pubsub topic for deadletter output")
    String getDeadLetterTopic();

    void setDeadLetterTopic(String value);

    @Description("Schema check sample ratio")
    @Default.Float(0.01f)
    float getSchemaCheckRatio();

    void setSchemaCheckRatio(float value);

    @Description("Streamprocessor version")
    String getVersion();

    void setVersion(String value);

    @Description("Data contracts base api url")
    String getDataContractsServiceUrl();

    void setDataContractsServiceUrl(String value);

    @Description("Relaxed strictness for data types")
    @Default.Boolean(false)
    Boolean getRelaxedStrictness();

    void setRelaxedStrictness(Boolean value);
}
