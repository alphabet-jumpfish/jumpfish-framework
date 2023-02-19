package io.github.alphabet.message.listener.pulsar;


import io.github.alphabet.message.listener.core.common.ServiceThread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PulsarMQConsumerThread<Message> extends ServiceThread {

    private String serviceName;

    public PulsarMQConsumerThread(String serviceName) {
        this.serviceName = serviceName;
    }

    private Function<org.apache.pulsar.client.api.Message, Message> convertor;

    private Consumer<List<Message>> execute;

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    private volatile org.apache.pulsar.client.api.Consumer pulsarConsumer;

    public void initialization(
            Function<org.apache.pulsar.client.api.Message, Message> convertor,
            org.apache.pulsar.client.api.Consumer pulsarConsumer,
            Consumer<List<Message>> execute
    ) {
        this.pulsarConsumer = pulsarConsumer;
        this.convertor = convertor;
        this.execute = execute;
    }

    private boolean checkInitialization() {
        if (convertor == null || execute == null
                || pulsarConsumer == null) {
            return false;
        }
        return true;
    }

    @Override
    public void run() {

        while (!isStopped()) {
            this.running();
        }

    }

    // @author wangjiawen explain method
    // description:
    // take original data form the rocketmq-client of default-pull-consumer
    // use juf.consumer to calculate messages
    public void running() {

        if (!this.checkInitialization()) {
            makeStop();
            return;
        }

        try {

            List<org.apache.pulsar.client.api.Message> messages = new ArrayList<>();
            Iterator<org.apache.pulsar.client.api.Message> iterator = pulsarConsumer.batchReceive().iterator();
            while (iterator.hasNext()) {
                messages.add(iterator.next());
            }

            if (messages.isEmpty()) {
                return;
            }

            List<Message> convertorMessage = messages.stream().map(param -> convertor.apply(param)).collect(Collectors.toList());
            execute.accept(convertorMessage);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

    }


}
