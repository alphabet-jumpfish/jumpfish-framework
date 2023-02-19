package io.github.alphabet.message.listener.pulsar.listener;

import java.lang.reflect.Method;


public class MethodPulsarMQListenerEndpoint extends AbstractPulsarMQListenerEndpoint {

    private Object bean;

    private Method method;

    private String beanName;

    @Override
    public Object getBean() {
        return bean;
    }

    public void setBean(Object bean) {
        this.bean = bean;
    }

    @Override
    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    @Override
    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

}
