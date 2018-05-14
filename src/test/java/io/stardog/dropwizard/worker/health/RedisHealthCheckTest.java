package io.stardog.dropwizard.worker.health;

import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisHealthCheckTest {

    @Test
    public void check() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.getClient()).thenReturn(new Client());

        when(jedis.ping()).thenReturn("PONG");
        RedisHealthCheck check = new RedisHealthCheck(jedis);
        assertTrue(check.check().isHealthy());

        when(jedis.ping()).thenReturn("PING");
        assertFalse(check.check().isHealthy());

        when(jedis.ping()).thenThrow(new RuntimeException());
        assertFalse(check.check().isHealthy());
    }

    @Test
    public void getHostPort() {
        RedisHealthCheck check = new RedisHealthCheck(new Jedis("localhost", 6379));
        assertEquals("localhost:6379", check.getHostPort());
    }
}