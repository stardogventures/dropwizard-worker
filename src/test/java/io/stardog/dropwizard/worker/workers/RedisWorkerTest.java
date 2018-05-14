package io.stardog.dropwizard.worker.workers;

import com.google.common.collect.ImmutableList;
import io.stardog.dropwizard.worker.WorkMethods;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.data.WorkMethod;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisWorkerTest {
    private AtomicBoolean didWork = new AtomicBoolean(false);

    @Test
    public void processMessage() {
        WorkMethods workMethods = WorkMethods.of(ImmutableList.of(
                WorkMethod.of("test", (params) -> didWork.set(true))
        ));
        RedisWorker worker = new RedisWorker(workMethods, mock(JedisPool.class), "channel");
        worker.processMessage(WorkMessage.of("test"));

        assertTrue(didWork.get());
    }
}