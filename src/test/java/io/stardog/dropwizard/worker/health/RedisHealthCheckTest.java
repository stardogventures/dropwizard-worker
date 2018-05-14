package io.stardog.dropwizard.worker.health;

import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisHealthCheckTest {

    @Test
    public void check() {
        JedisPool jedisPool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(jedis.getClient()).thenReturn(new Client());
        when(jedisPool.getResource()).thenReturn(jedis);

        when(jedis.ping()).thenReturn("PONG");
        RedisHealthCheck check = new RedisHealthCheck(jedisPool);
        assertTrue(check.check().isHealthy());

        when(jedis.ping()).thenReturn("PING");
        assertFalse(check.check().isHealthy());

        when(jedis.ping()).thenThrow(new RuntimeException());
        assertFalse(check.check().isHealthy());
    }

    @Test
    public void getHostPort() {
        RedisHealthCheck check = new RedisHealthCheck(mock(JedisPool.class));
        assertEquals("localhost:6379", check.getHostPort(new Jedis("localhost", 6379)));
    }
}