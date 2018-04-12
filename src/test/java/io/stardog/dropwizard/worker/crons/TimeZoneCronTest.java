package io.stardog.dropwizard.worker.crons;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class TimeZoneCronTest {

    @Test
    public void accept() {
        InnerCron innerCron = new InnerCron();
        assertEquals(0, innerCron.getCounter());

        TimeZoneCron cron = TimeZoneCron.of(innerCron, ZoneId.of("America/New_York"));

        // no dst passed, should always run the inner cron
        cron.accept(ImmutableMap.of());
        assertEquals(1, innerCron.getCounter());

        // dst passed with invalid value, should throw exception
        try {
            cron.accept(ImmutableMap.of("dst", "badvalue"));
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // okay
        }

        // not DST, should not run when dst is set to true, should run when dst is set to false
        cron.accept(ImmutableMap.of("dst", true), Instant.parse("2018-12-23T09:30:00Z"));
        assertEquals(1, innerCron.getCounter());
        cron.accept(ImmutableMap.of("dst", false), Instant.parse("2018-12-23T09:30:00Z"));
        assertEquals(2, innerCron.getCounter());

        // DST, should run when dst is set to true, should not run when dst is set to false
        cron.accept(ImmutableMap.of("dst", true), Instant.parse("2018-07-23T09:30:00Z"));
        assertEquals(3, innerCron.getCounter());
        cron.accept(ImmutableMap.of("dst", false), Instant.parse("2018-07-23T09:30:00Z"));
        assertEquals(3, innerCron.getCounter());
    }
}

class InnerCron implements Consumer<Map<String,Object>> {
    private int counter = 0;

    @Override
    public void accept(Map<String, Object> params) {
        counter++;
    }

    public int getCounter() {
        return counter;
    }
}