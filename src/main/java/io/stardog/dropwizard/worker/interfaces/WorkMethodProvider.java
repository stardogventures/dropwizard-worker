package io.stardog.dropwizard.worker.interfaces;

import io.stardog.dropwizard.worker.data.WorkMethod;

public interface WorkMethodProvider {
    WorkMethod getWork(String methodName);
}
