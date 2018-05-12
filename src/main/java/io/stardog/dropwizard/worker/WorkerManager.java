package io.stardog.dropwizard.worker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.data.WorkerConfig;
import io.stardog.dropwizard.worker.interfaces.ManagedWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class WorkerManager implements Managed {
    private final String name;
    private final ManagedWorker worker;
    private final WorkerConfig config;
    private final MetricRegistry metrics;

    private final AtomicInteger workerCount = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> sleeper = null;
    private volatile boolean isRunning = false;
    private long currentIntervalMillis;

    private final static Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);

    @Inject
    public WorkerManager(String name, WorkerConfig config, ManagedWorker worker, MetricRegistry metrics) {
        this.name = name;
        this.worker = worker;
        this.config = config;
        this.metrics = metrics;

        this.scheduler = Executors.newScheduledThreadPool(config.getMaxThreads());
        this.currentIntervalMillis = config.getMinIntervalMillis();
    }

    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting " + name + " with config: " + config);

        worker.start();

        workerCount.set(1);
        metrics.register(MetricRegistry.name(WorkerManager.class, name, "workers"),
                (Gauge<Integer>) ()-> workerCount.get());
        metrics.register(MetricRegistry.name(WorkerManager.class, name, "interval"),
                (Gauge<Long>) ()-> currentIntervalMillis);

        isRunning = true;
        submitProcessMessages();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Starting shutdown");

        isRunning = false;
        if (sleeper != null) {
            LOGGER.debug("Cancelling sleeper thread");
            sleeper.cancel(false);
        }

        LOGGER.debug("Shutting down worker");
        worker.stop();

        LOGGER.debug("Shutting down scheduler");
        scheduler.shutdown();
        LOGGER.debug("Awaiting termination for " + config.getMaxShutdownMillis() + "ms");
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
            LOGGER.debug("Scheduling next processMessages in " + millis + "ms");
            sleeper = scheduler.schedule(() -> { this.processMessages(); }, millis, TimeUnit.MILLISECONDS);
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
                boolean needsMoreWorkers = worker.processMessages();
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
            LOGGER.debug("Increasing workerCount to " + currentCount);
            submitProcessMessages();
        }
        currentIntervalMillis = config.getMinIntervalMillis();
    }

    protected void decWorkers() {
        int currentCount = workerCount.decrementAndGet();
        LOGGER.debug("Decreasing workerCount to " + currentCount);

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
