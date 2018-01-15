package io.stardog.dropwizard.worker.workers;

import io.stardog.dropwizard.worker.WorkerService;
import io.stardog.dropwizard.worker.data.WorkMessage;
import io.stardog.dropwizard.worker.interfaces.Worker;

import javax.inject.Singleton;
import java.util.LinkedList;
import java.util.Queue;

@Singleton
public class LocalWorker implements Worker {
    private final Queue<WorkMessage> queue;

    public LocalWorker() {
        this(new LinkedList<>());
    }

    public LocalWorker(Queue<WorkMessage> queue) {
        this.queue = queue;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public boolean processMessages(WorkerService service) {
        WorkMessage message = queue.poll();
        if (message == null) {
            return false;
        }

        service.getWorkMethod(message.getMethod()).getConsumer().accept(message.getParams());

        return true;
    }

    public void submitMessage(WorkMessage message) {
        queue.add(message);
    }
}
