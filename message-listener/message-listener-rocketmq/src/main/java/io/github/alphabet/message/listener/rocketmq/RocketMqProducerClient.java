package io.github.alphabet.message.listener.rocketmq;

import io.github.alphabet.message.listener.core.properties.RocketMQProperties;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;


// @author wangjiawen explain
// description:
// according to class construct method to loading RocketMqProducerClient form the application-context
// then
// provide feature sendMessage method
@ConditionalOnProperty(prefix = RocketMQProperties.ROCKET_MQ_PREFIX, value = "address")
public class RocketMqProducerClient implements InitializingBean, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private RocketMQProperties rocketMQProperties;
    private ConcurrentHashMap<String, DefaultMQProducer> topicProducerMap = new ConcurrentHashMap<>();

    public SendResult sendMessage(String topic, String message) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer defaultMQProducer = topicProducerMap.get(topic);
        if (defaultMQProducer == null) {
            defaultMQProducer = createMQProducer(topic);
        }
        SendResult sendResult = defaultMQProducer.send(
                new Message(topic, message.getBytes(StandardCharsets.UTF_8))
        );
        return sendResult;
    }

    // send method
//    public SendResult sendMessage(Message message) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
//        SendResult sendResult = defaultMQProducer.send(message);
//        return sendResult;
//    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.rocketMQProperties = applicationContext.getBean(RocketMQProperties.class);
    }

    private DefaultMQProducer createMQProducer(String topic) {

        DefaultMQProducer producer = new DefaultMQProducer("default-client-driver-producer-" + topic);

        AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
                new SessionCredentials(rocketMQProperties.getUserName(), rocketMQProperties.getPassword())
        );

        if (!StringUtils.isEmpty(rocketMQProperties.getUserName()) && !StringUtils.isEmpty(rocketMQProperties.getPassword())) {
            producer = new DefaultMQProducer("default-client-driver-producer", aclClientRPCHook);
        }

        producer.setNamesrvAddr(rocketMQProperties.getAddress());
        try {
            producer.start();
            topicProducerMap.put(topic, producer);
        } catch (MQClientException e) {
            e.printStackTrace();
        } finally {

        }
        return producer;
    }

    @Deprecated
    private DefaultMQProducer install() {
        DefaultMQProducer producer = new DefaultMQProducer("default-client-driver-producer");

        AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
                new SessionCredentials(rocketMQProperties.getUserName(), rocketMQProperties.getPassword())
        );

        if (!StringUtils.isEmpty(rocketMQProperties.getUserName()) && !StringUtils.isEmpty(rocketMQProperties.getPassword())) {
            producer = new DefaultMQProducer("default-client-driver-producer", aclClientRPCHook);
        }

        producer.setNamesrvAddr(rocketMQProperties.getAddress());
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        } finally {

        }
        return producer;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
