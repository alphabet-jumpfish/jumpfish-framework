package io.github.alphabet.message.listener.rocketmq;

import io.github.alphabet.message.listener.core.support.Acknowledgment;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;

public class RocketMQAcknowledgment implements Acknowledgment {

    private DefaultLitePullConsumer consumer;

    public RocketMQAcknowledgment(DefaultLitePullConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void acknowledge() {
        consumer.commitSync();
    }
}
