package io.stardog.dropwizard.worker.data;

import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.function.Consumer;

@AutoValue
public abstract class WorkMethod {
    public abstract String getMethod();
    public abstract Consumer<Map<String,Object>> getConsumer();

    public static WorkMethod of(String method, Consumer<Map<String,Object>> consumer) {
        return new AutoValue_WorkMethod(method, consumer);
    }
}
