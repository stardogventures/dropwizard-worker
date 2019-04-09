package io.stardog.dropwizard.worker.examples;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import io.stardog.dropwizard.worker.WorkMethods;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.health.RedisHealthCheck;
import io.stardog.dropwizard.worker.senders.RedisSender;
import io.stardog.dropwizard.worker.workers.RedisWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisExample {
    private final static Logger LOGGER = LoggerFactory.getLogger(RedisExample.class);

    public static void main(String[] arg) throws Exception {
        WorkMethods workMethods = WorkMethods.of(ImmutableList.of(
                WorkMethod.of("ping", params -> { LOGGER.info("Received pong"); })
        ));

        JedisPool jedisPool = new JedisPool("localhost", 6379);

        RedisWorker worker = new RedisWorker(workMethods, jedisPool, "example");
        worker.start();

        LOGGER.info("Sending ping");

        RedisSender sender = new RedisSender(jedisPool, "example");
        sender.send(WorkMessage.of("ping"));

        LOGGER.info("Sending another ping");
        sender.send(WorkMessage.of("ping"));

        LOGGER.info("Sleeping for 5 seconds...");

        Thread.sleep(5000);

        LOGGER.info("Shutting down");

        worker.stop();
    }
}
