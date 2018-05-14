package io.stardog.dropwizard.worker.health;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RedisHealthCheck extends HealthCheck {
    private final Jedis jedis;
    private final Logger LOGGER = LoggerFactory.getLogger(RedisHealthCheck.class);

    @Inject
    public RedisHealthCheck(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    protected Result check() {
        try {
            String pong = jedis.ping();
            if (!"PONG".equals(pong)) {
                String hostPort = getHostPort();
                LOGGER.warn("Returned unexpected ping result from " + hostPort + ": " + pong);
                return Result.unhealthy("Returned unexpected ping result: " + pong);
            } else {
                return Result.healthy();
            }
        } catch (Exception e) {
            String hostPort = getHostPort();
            LOGGER.warn("Exception trying to ping Redis at " + hostPort, e);
            return Result.unhealthy("Exception trying to ping Redis at " + hostPort + ": " + e.getMessage());
        }
    }

    protected String getHostPort() {
        return jedis.getClient().getHost() + ":" + jedis.getClient().getPort();
    }
}
