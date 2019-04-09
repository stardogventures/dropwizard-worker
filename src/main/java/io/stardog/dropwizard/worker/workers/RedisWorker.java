package io.stardog.dropwizard.worker.workers;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.WorkMethods;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.util.WorkerDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Singleton
public class RedisWorker implements Managed {
    private final WorkMethods workMethods;
    private final JedisPool jedisPool;
    private final String channel;
    private final ObjectMapper mapper;
    private final MetricRegistry metrics;
    private final ExecutorService executorService;
    private final Logger LOGGER = LoggerFactory.getLogger(RedisWorker.class);
    private JedisPubSub subscriber;

    @Inject
    public RedisWorker(WorkMethods workMethods, JedisPool jedisPool, @Named("redisWorkerChannel") String channel, MetricRegistry metrics, ObjectMapper mapper) {
        this.workMethods = workMethods;
        this.jedisPool = jedisPool;
        this.channel = channel;
        this.mapper = mapper;
        this.metrics = metrics;
        executorService = Executors.newFixedThreadPool(1);
    }

    public RedisWorker(WorkMethods workMethods, JedisPool jedisPool, String channel) {
        this(workMethods, jedisPool, channel, new MetricRegistry(), WorkerDefaults.MAPPER);
    }

    public RedisWorker(WorkMethods workMethods, JedisPool jedisPool, String channel, MetricRegistry metrics) {
        this(workMethods, jedisPool, channel, metrics, WorkerDefaults.MAPPER);
    }

    @Override
    public void start() {
        try {
            LOGGER.info("Subscribing to channel " + channel);
            executorService.submit(() -> {
                subscriber = new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String messageString) {
                        super.onMessage(channel, messageString);

                        WorkMessage message;
                        try {
                            message = mapper.readValue(messageString, WorkMessage.class);
                        } catch (Exception e) {
                            LOGGER.warn("Exception parsing: " + messageString);
                            metrics.meter(MetricRegistry.name(RedisWorker.class, channel, "error")).mark();
                            metrics.meter(MetricRegistry.name(RedisWorker.class, channel, "error", "parse")).mark();
                            return;
                        }

                        try {
                            processMessage(message);
                        } catch (Exception e) {
                            LOGGER.warn("Exception processing: " + messageString, e);
                            metrics.meter(MetricRegistry.name(SqsWorker.class, channel, "error")).mark();
                            metrics.meter(MetricRegistry.name(SqsWorker.class, channel, "error", message.getMethod())).mark();
                        }
                    }
                };
                jedisPool.getResource().subscribe(subscriber, channel);
            });
        } catch (Exception e) {
            LOGGER.warn("Unable to subscribe to redis", e);
        }
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Unsubscribing from channel " + channel);
        subscriber.unsubscribe();
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        executorService.shutdownNow();
        LOGGER.info("Stopped");
    }

    protected void processMessage(WorkMessage message) {
        long startTime = System.currentTimeMillis();

        if (message.getQueueAt().isPresent()) {
            long queueTime = message.getQueueAt().get().toEpochMilli();
            long delayTime = Math.max(0, queueTime - startTime);
            metrics.timer(MetricRegistry.name(RedisWorker.class, channel, "delay"))
                    .update(delayTime, TimeUnit.MILLISECONDS);
        }

        WorkMethod workMethod = workMethods.getMethod(message.getMethod());
        workMethod.getFunction().apply(message.getParams());
        long endTime = System.currentTimeMillis();

        metrics.timer(MetricRegistry.name(RedisWorker.class, channel, "timer", message.getMethod()))
                .update(endTime - startTime, TimeUnit.MILLISECONDS);
    }
}
