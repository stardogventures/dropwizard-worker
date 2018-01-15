package io.stardog.dropwizard.worker.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class WorkerConfigTest {
    @Test
    public void testDefault() throws Exception {
        WorkerConfig config = WorkerConfig.builder().build();
        assertEquals(10, config.getMaxThreads());
        assertEquals(60000L, config.getMaxIntervalMillis());

        WorkerConfig override = WorkerConfig.builder().maxThreads(50).build();
        assertEquals(50, override.getMaxThreads());
    }

    @Test
    public void testDeserialize() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = "{\"maxThreads\":100}";
        WorkerConfig config = mapper.readValue(json, WorkerConfig.class);
        assertEquals(100, config.getMaxThreads());
    }
}