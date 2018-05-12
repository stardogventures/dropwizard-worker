package io.stardog.dropwizard.worker;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.data.WorkerConfig;
import io.stardog.dropwizard.worker.workers.LocalWorker;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WorkerServiceTest {
    @Test
    public void testRun() throws Exception {
        final AtomicBoolean didWork = new AtomicBoolean(false);

        WorkerConfig config = WorkerConfig.builder().incIntervalMillis(10).build();
        WorkMethods methods = WorkMethods.of(ImmutableList.of(
                WorkMethod.of("test-work", (params) -> { didWork.set(true); } )
        ));
        LocalWorker testWorker = new LocalWorker(methods);
        WorkerManager service = new WorkerManager(
                "worker-service", config, testWorker, new MetricRegistry());

        assertFalse(service.isRunning());
        service.start();

        testWorker.submitMessage(WorkMessage.of("test-work"));

        while (!didWork.get()) {
            // await the completion of the work
        }

        assertTrue(didWork.get());
        assertTrue(service.isRunning());

        service.stop();

        assertFalse(service.isRunning());
    }
}