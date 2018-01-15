package io.stardog.dropwizard.worker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.data.WorkerConfig;
import io.stardog.dropwizard.worker.interfaces.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class WorkerService implements Managed {
    private final String name;
    private final Map<String,WorkMethod> workMap;
    private final Worker worker;
    private final WorkerConfig config;
    private final MetricRegistry metrics;

    private final AtomicInteger workerCount = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> sleeper = null;
    private volatile boolean isRunning = false;
    private long currentIntervalMillis;

    private final static Logger LOGGER = LoggerFactory.getLogger(WorkerService.class);

    @Inject
    public WorkerService(String name, WorkerConfig config, Collection<WorkMethod> methods, Worker worker, MetricRegistry metrics) {
        this.name = name;
        this.worker = worker;
        this.config = config;
        this.metrics = metrics;

        ImmutableMap.Builder<String,WorkMethod> builder = ImmutableMap.builderWithExpectedSize(methods.size());
        for (WorkMethod w : methods) {
            builder.put(w.getMethod(), w);
        }
        this.workMap = builder.build();

        this.scheduler = Executors.newScheduledThreadPool(config.getMaxThreads());
        this.currentIntervalMillis = config.getMinIntervalMillis();
    }

    public boolean isRunning() {
        return isRunning;
    }

    public boolean isMethod(String methodName) {
        return workMap.containsKey(methodName);
    }

    public WorkMethod getWorkMethod(String methodName) {
        if (workMap.containsKey(methodName)) {
            return workMap.get(methodName);
        } else {
            throw new IllegalArgumentException("Method not found: " + methodName);
        }
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting " + name + " with config: " + config);

        worker.start();

        workerCount.set(1);
        metrics.register(MetricRegistry.name(WorkerService.class, name, "workers"),
                (Gauge<Integer>) ()-> workerCount.get());
        metrics.register(MetricRegistry.name(WorkerService.class, name, "interval"),
                (Gauge<Long>) ()-> currentIntervalMillis);

        isRunning = true;
        submitProcessMessages();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Starting shutdown");

        isRunning = false;
        if (sleeper != null) {
            sleeper.cancel(false);
        }

        worker.stop();

        scheduler.shutdown();
        scheduler.awaitTermination(config.getMaxShutdownMillis(), TimeUnit.MILLISECONDS);
        scheduler.shutdownNow();

        LOGGER.info("Finished shutdown");
    }

    protected void submitProcessMessages() {
        try {
            scheduler.submit(() -> { this.processMessages(); });
        } catch (Exception e) {
            if (!isRunning) {
                return;
            } else {
                LOGGER.error("Unexpected exception submitting", e);
            }
        }
    }

    protected void scheduleProcessMessages(long millis) {
        try {
            scheduler.schedule(() -> { this.processMessages(); }, millis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (!isRunning) {
                return;
            } else {
                LOGGER.error("Unexpected exception scheduling", e);
            }
        }
    }

    protected void processMessages() {
        while (isRunning) {
            try {
                boolean needsMoreWorkers = worker.processMessages(this);
                if (needsMoreWorkers) {
                    incWorkers();
                } else {
                    decWorkers();
                    return;
                }
            } catch (Exception e) {
                LOGGER.error("Uncaught exception in worker", e);
                decWorkers();
                return;
            }
        }
    }

    protected void incWorkers() {
        if (workerCount.get() >= config.getMaxThreads()) {
            return;
        }
        int currentCount = workerCount.incrementAndGet();
        if (currentCount > config.getMaxThreads()) {
            workerCount.decrementAndGet();
        } else {
            submitProcessMessages();
        }
        currentIntervalMillis = config.getMinIntervalMillis();
    }

    protected void decWorkers() {
        int currentCount = workerCount.decrementAndGet();

        // if decrementing would reduce the number of workers to 0, then schedule a sleeper thread on an interval
        if (currentCount <= 0) {
            currentCount = workerCount.incrementAndGet();
            if (currentCount == 1) {
                currentIntervalMillis = Math.min(currentIntervalMillis + config.getIncIntervalMillis(), config.getMaxIntervalMillis());
                scheduleProcessMessages(currentIntervalMillis);
            } else {
                workerCount.decrementAndGet();
            }
        }
    }
}
