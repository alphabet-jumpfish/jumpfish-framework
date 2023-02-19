package io.github.alphabet.message.listener.pulsar;

import io.github.alphabet.message.listener.core.common.ServiceThreadContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class PulsarMQServiceThreadContainer implements ServiceThreadContainer {

    @Override
    public ExecutorService build(int concurrency) {
        return Executors.newFixedThreadPool(concurrency);
    }

    @Override
    public ExecutorService searchOne(String id) {
        return container.get(id);
    }

    @Override
    public boolean remove(String id) {

        ExecutorService executorService = container.get(id);
        try {
            executorService.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            container.remove(id);
        }

        return true;
    }

    @Override
    public boolean add(String id, ExecutorService executorService) {
        container.put(id, executorService);
        return true;
    }

}
