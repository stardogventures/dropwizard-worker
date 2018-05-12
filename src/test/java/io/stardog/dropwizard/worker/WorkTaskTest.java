package io.stardog.dropwizard.worker;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.stardog.dropwizard.worker.data.WorkMethod;
import io.stardog.dropwizard.worker.data.WorkerConfig;
import io.stardog.dropwizard.worker.workers.LocalWorker;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class WorkTaskTest {
    @Test
    public void execute() throws Exception {
        final AtomicInteger didWork = new AtomicInteger(0);

        WorkMethods methods = WorkMethods.of(ImmutableList.of(
                WorkMethod.of("testMethod", (params) -> { didWork.set(Integer.parseInt(params.get("val").toString())); })
        ));
        WorkTask task = new WorkTask(methods);
        ImmutableMultimap<String,String> multimap = ImmutableMultimap.of(
                "method", "testMethod",
                "params", "{val:8}");
        StringWriter sw = new StringWriter();
        PrintWriter output = new PrintWriter(sw);
        task.execute(multimap, output);

        assertEquals(8, didWork.get());
        assertTrue(sw.toString().startsWith("Running testMethod({\"val\":8})\nCompleted in "));
    }

}