package io.stardog.dropwizard.worker.senders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.interfaces.Sender;
import io.stardog.dropwizard.worker.util.WorkerDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;
import java.io.UncheckedIOException;

public class RedisSender implements Sender {
    private final Jedis jedis;
    private final String defaultChannel;
    private final ObjectMapper mapper;
    private final Logger LOGGER = LoggerFactory.getLogger(RedisSender.class);

    @Inject
    public RedisSender(Jedis jedis, String defaultChannel, ObjectMapper mapper) {
        this.jedis = jedis;
        this.defaultChannel = defaultChannel;
        this.mapper = mapper;
    }

    public RedisSender(Jedis jedis, String defaultChannel) {
        this(jedis, defaultChannel, WorkerDefaults.MAPPER);
    }

    @Override
    public void send(WorkMessage message) {
        send(message, defaultChannel);
    }

    public void send(WorkMessage message, String channel) {
        try {
            String payload = mapper.writeValueAsString(message);
            jedis.publish(channel, payload);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
