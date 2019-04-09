package io.stardog.dropwizard.worker.workers;

import io.stardog.dropwizard.worker.WorkMethods;
import io.stardog.dropwizard.worker.WorkerManager;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.interfaces.ManagedWorker;

import javax.inject.Singleton;
import java.util.LinkedList;
import java.util.Queue;

@Singleton
public class LocalWorker implements ManagedWorker {
    private final WorkMethods methods;
    private final Queue<WorkMessage> queue;

    public LocalWorker(WorkMethods methods) {
        this(methods, new LinkedList<>());
    }

    public LocalWorker(WorkMethods methods, Queue<WorkMessage> queue) {
        this.methods = methods;
        this.queue = queue;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public boolean processMessages() {
        WorkMessage message = queue.poll();
        if (message == null) {
            return false;
        }

        methods.getMethod(message.getMethod()).getFunction().apply(message.getParams());

        return true;
    }

    public void submitMessage(WorkMessage message) {
        queue.add(message);
    }
}
