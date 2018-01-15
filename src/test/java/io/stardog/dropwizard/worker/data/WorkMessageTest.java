package io.stardog.dropwizard.worker.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class WorkMessageTest {
    @Test
    public void jsonSerialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        String json = "{'method':'methodName','params':{'param1':'value'},'at':1515531600000}";
        WorkMessage message = mapper.readValue(json, WorkMessage.class);
        assertEquals("methodName", message.getMethod());
        assertEquals(ImmutableMap.of("param1", "value"), message.getParams());
        assertEquals(Instant.ofEpochMilli(1515531600000L), message.getQueueAt().get());

        String toJson = mapper.writeValueAsString(message);
        assertEquals(json.replace('\'', '"'), toJson);
    }
}