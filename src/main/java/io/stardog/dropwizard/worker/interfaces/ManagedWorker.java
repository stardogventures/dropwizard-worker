package io.stardog.dropwizard.worker.interfaces;

import io.dropwizard.lifecycle.Managed;
import io.stardog.dropwizard.worker.WorkerManager;

public interface ManagedWorker extends Managed {
    boolean processMessages();
}