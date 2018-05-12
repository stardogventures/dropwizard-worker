package io.stardog.dropwizard.worker;

import io.stardog.dropwizard.worker.data.WorkMethod;

import java.util.HashMap;
import java.util.Map;

public class WorkMethods {
    private final Map<String,WorkMethod> methodMap;

    private WorkMethods(Map<String, WorkMethod> methodMap) {
        this.methodMap = methodMap;
    }

    public static WorkMethods of(Iterable<WorkMethod> methods) {
        Map<String,WorkMethod> map = new HashMap<>();
        for (WorkMethod m : methods) {
            map.put(m.getMethod(), m);
        }
        return new WorkMethods(map);
    }

    public boolean isMethod(String name) {
        return methodMap.containsKey(name);
    }

    public WorkMethod getMethod(String name) {
        if (!methodMap.containsKey(name)) {
            throw new IllegalArgumentException("Unknown work method: " + name);
        }
        return methodMap.get(name);
    }
}
