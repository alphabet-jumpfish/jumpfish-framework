package io.github.alphabet.message.listener.pulsar.listener;

import io.github.alphabet.message.listener.core.listener.ListenerEndpoint;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Collection;

public interface PulsarMQListenerEndpoint extends ListenerEndpoint {

    String getId();

    String getSubscriptionName();

    Collection<String> getTopics();

    int getConcurrency();

    // 消息类型
    SubscriptionType getSubscriptionType();

    // 批量消费数据大小
    int getConsumeMessageBatchMaxSize();



}
