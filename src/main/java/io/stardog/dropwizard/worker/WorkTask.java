package io.stardog.dropwizard.worker;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.stardog.dropwizard.worker.data.WorkMethod;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dropwizard Task for manually running work on demand.
 *
 * To use, make an HTTP post passing "method" and, optionally, "params" parameters.
 *
 * Example:
 *
 *   curl -X POST "http://localhost:8081/tasks/work?method=myMethod";
 */
@Singleton
public class WorkTask extends Task {
    private final WorkerService service;
    private final static ObjectMapper MAPPER = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    @Inject
    public WorkTask(WorkerService service) {
        this("work", service);
    }

    public WorkTask(String name, WorkerService service) {
        super(name);
        this.service = service;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> taskParams, PrintWriter writer) throws Exception {
        if (!taskParams.containsKey("method")) {
            writer.println("Must specify parameter: method");
            return;
        }

        String methodName = (String)((List)taskParams.get("method")).get(0);
        if (!service.isMethod(methodName)) {
            writer.println("Invalid method: " + methodName);
            return;
        }

        WorkMethod workMethod = service.getWorkMethod(methodName);
        Map<String,Object> params = ImmutableMap.of();
        if (taskParams.containsKey("params")) {
            String paramsJson = (String)((List)taskParams.get("params")).get(0);
            params = MAPPER.readValue(paramsJson, new TypeReference<HashMap<String, Object>>() {});
        }

        writer.println("Running " + methodName + "(" + MAPPER.writeValueAsString(params) + ")");
        long startTime = System.currentTimeMillis();
        workMethod.getConsumer().accept(params);
        long elapsedTime = System.currentTimeMillis() - startTime;
        writer.println("Completed in " + elapsedTime + "ms");
    }
}
