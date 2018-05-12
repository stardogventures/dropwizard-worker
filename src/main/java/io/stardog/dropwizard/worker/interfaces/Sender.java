package io.stardog.dropwizard.worker.interfaces;

import io.stardog.dropwizard.worker.data.WorkMessage;

public interface Sender {
    void send(WorkMessage message);
}
