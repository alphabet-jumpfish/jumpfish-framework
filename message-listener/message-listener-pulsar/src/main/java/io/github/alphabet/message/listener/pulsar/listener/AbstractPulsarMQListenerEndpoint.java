package io.github.alphabet.message.listener.pulsar.listener;


import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public abstract class AbstractPulsarMQListenerEndpoint implements PulsarMQListenerEndpoint {

    private final Collection<String> topics = new ArrayList<>();

    private String id;

    private String subscriptionName;

    private int concurrency = 1;

    private SubscriptionType subscriptionType;

    private boolean autoCommitACK;

    private int consumeMessageBatchMaxSize;

    @Override
    public Collection<String> getTopics() {
        return topics;
    }

    public void setTopics(String... topics) {
        Assert.notNull(topics, "'topics' must not be null ");
        this.topics.clear();
        this.topics.addAll(Arrays.asList(topics));
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }


    @Override
    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public void setSubscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    @Override
    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public boolean isAutoCommitACK() {
        return autoCommitACK;
    }

    public void setAutoCommitACK(boolean autoCommitACK) {
        this.autoCommitACK = autoCommitACK;
    }
}
