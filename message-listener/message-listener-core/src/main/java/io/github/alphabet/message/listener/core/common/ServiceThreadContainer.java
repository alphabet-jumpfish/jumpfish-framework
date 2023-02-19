package io.github.alphabet.message.listener.core.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public interface ServiceThreadContainer {

    public Map<String, ExecutorService> container = new HashMap<>();

    ExecutorService build(int concurrency);

    ExecutorService searchOne(String id);

    boolean remove(String id);

    boolean add(String id, ExecutorService executorService);

}
