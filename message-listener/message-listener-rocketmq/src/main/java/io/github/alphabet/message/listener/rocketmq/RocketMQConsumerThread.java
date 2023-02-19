package io.github.alphabet.message.listener.rocketmq;

import io.github.alphabet.message.listener.core.common.ServiceThread;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class RocketMQConsumerThread<Message> extends ServiceThread {

    private String serviceName;

    public RocketMQConsumerThread(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    private volatile DefaultLitePullConsumer rocketmqConsumer;

    private volatile Function<org.apache.rocketmq.common.message.Message, Message> convertor;

    private volatile Consumer<List<Message>> execute;

    public void initialization(
            Function<org.apache.rocketmq.common.message.Message, Message> convertor,
            DefaultLitePullConsumer rocketmqConsumer,
            Consumer<List<Message>> execute
    ) {
        this.rocketmqConsumer = rocketmqConsumer;
        this.convertor = convertor;
        this.execute = execute;
    }

    private boolean checkInitialization() {
        if (convertor == null || execute == null
                || rocketmqConsumer == null) {
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
            List<Message> messages = rocketmqConsumer.poll().stream()
                    .map(param -> convertor.apply(param)).collect(Collectors.toList());

            if (messages.isEmpty()) {
                Thread.sleep(1L);
                return;
            }

            execute.accept(messages);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

    }


}
