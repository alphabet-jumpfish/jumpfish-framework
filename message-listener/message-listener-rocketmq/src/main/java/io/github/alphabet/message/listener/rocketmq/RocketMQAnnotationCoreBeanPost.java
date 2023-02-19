package io.github.alphabet.message.listener.rocketmq;

import io.github.alphabet.message.listener.core.MQAnnotationCoreBeanPost;
import io.github.alphabet.message.listener.core.anotations.DynamicListenerType;
import io.github.alphabet.message.listener.core.anotations.ListenerProcessorStrategy;
import io.github.alphabet.message.listener.core.anotations.MQListener;
import io.github.alphabet.message.listener.core.anotations.RocketMQListener;
import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import io.github.alphabet.message.listener.core.message.ConsumerRecords;
import io.github.alphabet.message.listener.core.properties.RocketMQProperties;
import io.github.alphabet.message.listener.rocketmq.listener.MethodRocketmqListenerEndpoint;
import io.github.alphabet.message.listener.rocketmq.message.RocketConsumerRecord;
import io.github.alphabet.message.listener.rocketmq.message.RocketConsumerRecords;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;

@Slf4j
@ConditionalOnProperty(prefix = RocketMQProperties.ROCKET_MQ_PREFIX, value = "address")
@ListenerProcessorStrategy(strategy = DynamicListenerType.ROCKETMQ)
public class RocketMQAnnotationCoreBeanPost implements MQAnnotationCoreBeanPost<RocketMQListener> {

    private ApplicationContext applicationContext;

    private RocketMQProperties rocketMQProperties;

    @Override
    public void processMQListener(MQListener listener, Method method, Object bean, String beanName) {

        MethodRocketmqListenerEndpoint endpoint = new MethodRocketmqListenerEndpoint();

        endpoint.setTopics(listener.topics());
        endpoint.setSubscriptionName(listener.consumer());
        endpoint.setConcurrency(Integer.parseInt(listener.concurrency()));
        endpoint.setMessageModel(MessageModel.CLUSTERING);
        endpoint.setConsumeMessageBatchMaxSize(100);
        endpoint.setAutoCommitACK(true);

        endpoint.setMethod(method);
        endpoint.setBean(bean);
        endpoint.setBeanName(beanName);
        endpoint.setId(bean.getClass().getName() + "#" + method.getName());

        this.processListenerEndpoint(endpoint);

    }

    @Override
    public void processAssignMQListener(MQListener listener, Method method, Object bean, String beanName) {

        final RocketMQListener rocketMQListener = listener.rocketMQListener();
        final String[] topics = rocketMQListener.topics();
        Assert.notNull(topics, "'topics' must not be null ");

        MethodRocketmqListenerEndpoint endpoint = new MethodRocketmqListenerEndpoint();
        endpoint.setTopics(rocketMQListener.topics());
        endpoint.setSubscriptionName(rocketMQListener.subscriptionName());
        endpoint.setConcurrency(Integer.parseInt(rocketMQListener.concurrency()));
        endpoint.setMessageModel(rocketMQListener.messageModel());
        endpoint.setConsumeMessageBatchMaxSize(rocketMQListener.consumeMessageBatchMaxSize());
        endpoint.setAutoCommitACK(rocketMQListener.autoCommitACK());

        endpoint.setMethod(method);
        endpoint.setBean(bean);
        endpoint.setBeanName(beanName);
        endpoint.setId(bean.getClass().getName() + "#" + method.getName());

        this.processListenerEndpoint(endpoint);

    }

    // @author wangjiawen explain class
    // core process
    // description:
    // simple implements threads according to cycle thread start
    public void processListenerEndpoint(MethodRocketmqListenerEndpoint endpoint) {

        int concurrency = endpoint.getConcurrency();

        DefaultLitePullConsumer consumer = this.install(endpoint);

        if (consumer == null) {
            return;
        }

        //

        // single thread
        RocketMQConsumerThread<Message> rocketMqConsumerThread = new RocketMQConsumerThread(endpoint.getBeanName());
        rocketMqConsumerThread.initialization(
                // function apply the object
                (message -> {
                    return message;
                }),
                // default-pull-consumer object
                consumer,
                // consumer logic
                (messages) -> {

                    Class<?>[] parameterTypes = endpoint.getMethod().getParameterTypes();
                    // method invoke
                    Object bean = endpoint.getBean();
                    Method method = endpoint.getMethod();

                    if (ConsumerRecord.class.isAssignableFrom(parameterTypes[0])) {

                        messages.stream().forEach(message -> {

                            RocketConsumerRecord record = new RocketConsumerRecord(null, new String(message.getBody()), message);

                            try {
                                if (method.getParameterCount() > 1) {
                                    method.invoke(bean, record, new RocketMQAcknowledgment(consumer));
                                } else {
                                    method.invoke(bean, record);
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error(endpoint.getId() + ":invoke failed exception:{}", e.getMessage());
                            } finally {
                                log.info(endpoint.getId() + ":invoke success");
                            }

                        });

                    }

                    if (ConsumerRecords.class.isAssignableFrom(parameterTypes[0])) {

                        RocketConsumerRecords records = new RocketConsumerRecords(messages);

                        try {

                            if (method.getParameterCount() > 1) {
                                method.invoke(bean, records, new RocketMQAcknowledgment(consumer));
                            } else {
                                method.invoke(bean, records);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            log.error(endpoint.getId() + ":invoke failed exception:{}", e.getMessage());
                        } finally {
                            log.info(endpoint.getId() + ":invoke success");
                        }
                    }

                }
        );

        // thread container
        RocketMQServiceThreadContainer rocketMQServiceThreadContainer = applicationContext.getBean(RocketMQServiceThreadContainer.class);
        ExecutorService executorService = rocketMQServiceThreadContainer.build(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(rocketMqConsumerThread);
        }
        rocketMQServiceThreadContainer.add(endpoint.getId(), executorService);

    }

    // @author wangjiawen explain method
    // consumer install process, when failed condition, install method return null.
    private DefaultLitePullConsumer install(MethodRocketmqListenerEndpoint endpoint) {

        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer();

        AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
                new SessionCredentials(rocketMQProperties.getUserName(), rocketMQProperties.getPassword())
        );

        if (!StringUtils.isEmpty(rocketMQProperties.getUserName()) && !StringUtils.isEmpty(rocketMQProperties.getPassword())) {
            consumer = new DefaultLitePullConsumer(aclClientRPCHook);
        }

        consumer.setConsumerGroup(endpoint.getSubscriptionName());
        consumer.setNamesrvAddr(rocketMQProperties.getAddress());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setPullBatchSize(endpoint.getConsumeMessageBatchMaxSize());
        consumer.setMessageModel(endpoint.getMessageModel());
        consumer.setAutoCommit(endpoint.isAutoCommitACK());

        for (String topic : endpoint.getTopics()) {
            try {
                consumer.subscribe(topic, "*");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {

            }
        }

        try {
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            if (consumer.isRunning()) {
                log.info(endpoint.getId() + ":consumer started success");
            } else {
                log.error(endpoint.getId() + ":consumer failed");
                return null;
            }
        }

        return consumer;
    }

    @Override
    public Boolean hasStrategy(MQListener listener) {
        return !ObjectUtils.isEmpty(listener.rocketMQListener().topics());
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.rocketMQProperties = applicationContext.getBean(RocketMQProperties.class);
    }
}
