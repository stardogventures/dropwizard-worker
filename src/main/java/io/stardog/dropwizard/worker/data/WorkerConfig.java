package io.stardog.dropwizard.worker.data;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;

@AutoValue
@JsonDeserialize(builder=AutoValue_WorkerConfig.Builder.class)
public abstract class WorkerConfig {
    public abstract int getMaxThreads();
    public abstract long getMinIntervalMillis();
    public abstract long getMaxIntervalMillis();
    public abstract long getIncIntervalMillis();
    public abstract long getMaxShutdownMillis();

    public abstract Builder toBuilder();
    public static WorkerConfig.Builder builder() { return new AutoValue_WorkerConfig.Builder(); }

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    public abstract static class Builder {
        Builder() {
            // defaults
            maxThreads(10);
            minIntervalMillis(0);
            maxIntervalMillis(60000L);
            incIntervalMillis(5000L);
            maxShutdownMillis(120000L);
        }
        public abstract Builder maxThreads(int maxThreads);
        public abstract Builder minIntervalMillis(long millis);
        public abstract Builder maxIntervalMillis(long millis);
        public abstract Builder incIntervalMillis(long millis);
        public abstract Builder maxShutdownMillis(long millis);
        public abstract WorkerConfig build();
    }
}
