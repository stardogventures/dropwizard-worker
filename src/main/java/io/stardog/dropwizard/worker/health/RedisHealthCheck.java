package io.stardog.dropwizard.worker.health;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RedisHealthCheck extends HealthCheck {
    private final JedisPool jedisPool;
    private final Logger LOGGER = LoggerFactory.getLogger(RedisHealthCheck.class);

    @Inject
    public RedisHealthCheck(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    protected Result check() {
        Jedis jedis = jedisPool.getResource();
        try {
            String pong = jedis.ping();
            jedis.close();
            if (!"PONG".equals(pong)) {
                String hostPort = getHostPort(jedis);
                LOGGER.warn("Returned unexpected ping result from " + hostPort + ": " + pong);
                return Result.unhealthy("Returned unexpected ping result: " + pong);
            } else {
                return Result.healthy();
            }
        } catch (Exception e) {
            String hostPort = getHostPort(jedis);
            LOGGER.warn("Exception trying to ping Redis at " + hostPort + ": ", e);
            return Result.unhealthy("Exception trying to ping Redis at " + hostPort + ": " + e.getMessage());
        }
    }

    protected String getHostPort(Jedis jedis) {
        return jedis.getClient().getHost() + ":" + jedis.getClient().getPort();
    }
}
