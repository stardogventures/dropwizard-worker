package io.stardog.dropwizard.worker.crons;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.function.Consumer;

public class TimeZoneCron implements Consumer<Map<String,Object>> {
    private final Consumer<Map<String,Object>> innerCron;
    private final ZoneId timezone;

    private TimeZoneCron(Consumer<Map<String, Object>> innerCron, ZoneId timezone) {
        this.innerCron = innerCron;
        this.timezone = timezone;
    }

    public static TimeZoneCron of(Consumer<Map<String,Object>> innerCron) {
        return of(innerCron, ZoneId.systemDefault());
    }

    public static TimeZoneCron of(Consumer<Map<String,Object>> innerCron, ZoneId timezone) {
        return new TimeZoneCron(innerCron, timezone);
    }

    @Override
    public void accept(Map<String, Object> params) {
        accept(params, Instant.now());
    }

    protected void accept(Map<String,Object> params, Instant now) {
        if (params.containsKey("dst") && params.get("dst") instanceof Boolean) {
            boolean dstRequired = Boolean.TRUE.equals(params.get("dst"));
            boolean isDst = timezone.getRules().isDaylightSavings(now);
            if (isDst == dstRequired) {
                innerCron.accept(params);
            }
        } else if (params.containsKey("dst")) {
            Object dstValue = params.get("dst");
            if (dstValue == null) {
                throw new IllegalArgumentException("Unexpected null value for dst");
            } else {
                throw new IllegalArgumentException("Unexpected value for dst - expected boolean, got " + dstValue + " of type " + dstValue.getClass());
            }
        } else {
            innerCron.accept(params);
        }
    }
}
