package io.stardog.dropwizard.worker.interfaces;

import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.WorkerService;

public interface Worker extends Managed {
    boolean processMessages(WorkerService service);
}