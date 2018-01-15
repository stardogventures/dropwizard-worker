package io.stardog.dropwizard.worker.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@AutoValue
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class WorkMessage {
    @JsonProperty("method")
    public abstract String getMethod();
    @JsonProperty("params")
    public abstract Map<String,Object> getParams();
    @JsonProperty("at")
    public abstract Optional<Instant> getQueueAt();

    public static WorkMessage of(String method) {
        return new AutoValue_WorkMessage(method, ImmutableMap.of(), Optional.empty());
    }

    public static WorkMessage of(String method, Map<String,Object> params) {
        return new AutoValue_WorkMessage(method, params, Optional.empty());
    }

    @JsonCreator
    public static WorkMessage of(@JsonProperty("method") String method,
                                 @JsonProperty("params") @Nullable Map<String,Object> params,
                                 @JsonProperty("at") @Nullable Instant queueAt) {
        return new AutoValue_WorkMessage(method, params != null ? params : ImmutableMap.of(), Optional.ofNullable(queueAt));
    }
}
