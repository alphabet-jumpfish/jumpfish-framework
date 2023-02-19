package io.github.alphabet.message.listener.rocketmq.listener;

import io.github.alphabet.message.listener.core.listener.ListenerEndpoint;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Collection;

public interface RocketmqListenerEndpoint extends ListenerEndpoint {

    String getId();

    String getSubscriptionName();

    Collection<String> getTopics();

    int getConcurrency();

    // 消息类型
    MessageModel getMessageModel();

    // 批量消费数据大小
    int getConsumeMessageBatchMaxSize();



}
