package io.stardog.dropwizard.worker.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@AutoValue
public abstract class WorkMethod {
    public abstract String getMethod();
    public abstract Function<Map<String,Object>,Boolean> getFunction();

    public static WorkMethod fn(String method, Function<Map<String,Object>,Boolean> func) {
        return new AutoValue_WorkMethod(method, func);
    }

    /** For backwards compatibility, continue to allow creation of WorkMethods with a consumer, and always return true **/
    public static WorkMethod of(String method, Consumer<Map<String,Object>> consumer) {
        return new AutoValue_WorkMethod(method, mapParams -> {
            consumer.accept(mapParams);
            return true;
        });
    }

    /** To make it easier to write methods that take a particular object format **/
    public static <T> WorkMethod fn(String method, ObjectMapper mapper, Class<T> klazz, Function<T,Boolean> func) {
        return new AutoValue_WorkMethod(method, mapParams -> {
            T object = mapper.convertValue(mapParams, klazz);
            return func.apply(object);
        });
    }
}
