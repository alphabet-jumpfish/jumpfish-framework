package io.github.alphabet.message.listener.core.listener;

import java.lang.reflect.Method;

public interface ListenerEndpoint {

    String getBeanName();

    Object getBean();

    Method getMethod();
}
