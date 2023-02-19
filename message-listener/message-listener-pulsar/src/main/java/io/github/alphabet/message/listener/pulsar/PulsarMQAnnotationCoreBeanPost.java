package io.github.alphabet.message.listener.pulsar;

import io.github.alphabet.message.listener.core.MQAnnotationCoreBeanPost;
import io.github.alphabet.message.listener.core.anotations.DynamicListenerType;
import io.github.alphabet.message.listener.core.anotations.ListenerProcessorStrategy;
import io.github.alphabet.message.listener.core.anotations.MQListener;
import io.github.alphabet.message.listener.core.anotations.PulsarMQListener;
import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import io.github.alphabet.message.listener.core.message.ConsumerRecords;
import io.github.alphabet.message.listener.core.properties.PulsarMQProperties;
import io.github.alphabet.message.listener.pulsar.listener.MethodPulsarMQListenerEndpoint;
import io.github.alphabet.message.listener.pulsar.message.PulsarConsumerRecord;
import io.github.alphabet.message.listener.pulsar.message.PulsarConsumerRecords;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

@Slf4j
@ConditionalOnProperty(prefix = PulsarMQProperties.PULSAR_MQ_PREFIX, value = "address")
@ListenerProcessorStrategy(strategy = DynamicListenerType.PULSAR)
public class PulsarMQAnnotationCoreBeanPost implements MQAnnotationCoreBeanPost<PulsarMQListener> {

    private ApplicationContext applicationContext;

    private PulsarMQProperties pulsarMQProperties;

    @Override
    public void processMQListener(MQListener listener, Method method, Object bean, String beanName) {

        MethodPulsarMQListenerEndpoint endpoint = new MethodPulsarMQListenerEndpoint();

        endpoint.setTopics(listener.topics());
        endpoint.setSubscriptionName(listener.consumer());
        endpoint.setConcurrency(Integer.parseInt(listener.concurrency()));
        endpoint.setSubscriptionType(SubscriptionType.Shared);
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

        final PulsarMQListener pulsarMQListener = listener.pulsarMQListener();
        final String[] topics = pulsarMQListener.topics();
        Assert.notNull(topics, "'topics' must not be null ");

        MethodPulsarMQListenerEndpoint endpoint = new MethodPulsarMQListenerEndpoint();
        endpoint.setTopics(pulsarMQListener.topics());
        endpoint.setSubscriptionName(pulsarMQListener.subscriptionName());
        endpoint.setConcurrency(Integer.parseInt(pulsarMQListener.concurrency()));
        endpoint.setSubscriptionType(pulsarMQListener.subscriptionType());
        endpoint.setAutoCommitACK(pulsarMQListener.autoCommitACK());

        endpoint.setMethod(method);
        endpoint.setBean(bean);
        endpoint.setBeanName(beanName);
        endpoint.setId(bean.getClass().getName() + "#" + method.getName());

        this.processListenerEndpoint(endpoint);

    }

    @Override
    public Boolean hasStrategy(MQListener listener) {
        return !ObjectUtils.isEmpty(listener.pulsarMQListener().topics());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.pulsarMQProperties = applicationContext.getBean(PulsarMQProperties.class);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    private void processListenerEndpoint(MethodPulsarMQListenerEndpoint endpoint) {

        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl("pulsar://" + pulsarMQProperties.getAddress()).build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        int concurrency = endpoint.getConcurrency();

        Consumer consumer = this.install(endpoint, pulsarClient);

        if (consumer == null) {
            return;
        }

        PulsarMQConsumerThread<Message> pulsarMQConsumerThread = new PulsarMQConsumerThread(endpoint.getBeanName());
        pulsarMQConsumerThread.initialization(
                // function apply the object
                (message -> {
                    return message;
                }),
                // object
                consumer,
                // consumer logic
                (messages) -> {

                    Class<?>[] parameterTypes = endpoint.getMethod().getParameterTypes();
                    // method invoke
                    Object bean = endpoint.getBean();
                    Method method = endpoint.getMethod();

                    if (ConsumerRecord.class.isAssignableFrom(parameterTypes[0])) {
                        messages.stream().forEach(message -> {
                            PulsarConsumerRecord record = new PulsarConsumerRecord(null, new String(message.getData()), message);
                            try {
                                if (method.getParameterCount() > 1) {
                                    method.invoke(bean, record, new ConsumerBatchAcknowledgment(consumer, new PulsarConsumerRecords(Arrays.asList(message))));
                                } else {
                                    method.invoke(bean, record);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error(endpoint.getId() + ":invoke failed exception:{}", e.getMessage());
                            } finally {
                                //log.info(endpoint.getId() + ":invoke success");
                            }
                        });
                    }

                    if (ConsumerRecords.class.isAssignableFrom(parameterTypes[0])) {
                        PulsarConsumerRecords records = new PulsarConsumerRecords(messages);
                        try {

                            if (method.getParameterCount() > 1) {
                                method.invoke(bean, records, new ConsumerBatchAcknowledgment(consumer, records));
                            } else {
                                method.invoke(bean, records);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            log.error(endpoint.getId() + ":invoke failed exception:{}", e.getMessage());
                        } finally {
                            //log.info(endpoint.getId() + ":invoke success");
                        }
                    }
                }
        );

        PulsarMQServiceThreadContainer pulsarMQServiceThreadContainer = applicationContext.getBean(PulsarMQServiceThreadContainer.class);
        ExecutorService executorService = pulsarMQServiceThreadContainer.build(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(pulsarMQConsumerThread);
        }
        pulsarMQServiceThreadContainer.add(endpoint.getId(), executorService);

    }

    private Consumer install(MethodPulsarMQListenerEndpoint endpoint, PulsarClient pulsarClient) {
        Consumer consumer = null;

        try {
            consumer = pulsarClient.newConsumer()
                    .topic(endpoint.getTopics().toArray(new String[0]))
                    .subscriptionName(endpoint.getSubscriptionName())
                    .subscriptionType(endpoint.getSubscriptionType())
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        return consumer;
    }

}
