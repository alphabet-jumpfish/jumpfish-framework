package io.github.alphabet.message.listener.pulsar;

import io.github.alphabet.message.listener.core.properties.PulsarMQProperties;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


// @author wangjiawen explain
// description:
// according to class construct method to loading RocketMqProducerClient form the application-context
// then
// provide feature sendMessage method
@ConditionalOnProperty(prefix = PulsarMQProperties.PULSAR_MQ_PREFIX, value = "address")
public class PulsarProducerClient implements InitializingBean, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private PulsarClient pulsarClient;
    private PulsarMQProperties pulsarMQProperties;

    public MessageId sendBytes(String topic, String message) throws PulsarClientException {
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        MessageId messageId = producer.send(message.getBytes(StandardCharsets.UTF_8));
        producer.close();
        return messageId;
    }

    private final ConcurrentHashMap<String, Producer> topicProducer = new ConcurrentHashMap();

    @SneakyThrows
    public <T> MessageId send(String topic, T message) {
        Class<?> clazz = message.getClass();
        Producer producer;
        if (Collection.class.isAssignableFrom(clazz)) {
            List messages = (List) message;
            clazz = messages.get(0).getClass();
            producer = getProducer(topic, clazz);
            for (Object o : messages) {
                producer.sendAsync(o);
            }
            return null;

        } else {
            producer = getProducer(topic, clazz);
            return producer.send(message);
        }

    }

    private Producer getProducer(String topic, Class clazz) throws PulsarClientException {
        if (topicProducer.containsKey(topic)) {
            return topicProducer.get(topic);
        }
        Producer producer;
        if (Collection.class.isAssignableFrom(clazz)) {
            producer = pulsarClient.newProducer().topic(topic).create();
        } else {
            producer = pulsarClient.newProducer(JSONSchema.of(clazz)).topic(topic).create();
        }
        topicProducer.put(topic, producer);
        return topicProducer.get(topic);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.pulsarMQProperties = applicationContext.getBean(PulsarMQProperties.class);
        this.pulsarClient = PulsarClient.builder().serviceUrl("pulsar://" + pulsarMQProperties.getAddress()).build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
