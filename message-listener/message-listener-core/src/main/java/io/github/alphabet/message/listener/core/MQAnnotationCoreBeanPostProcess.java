package io.github.alphabet.message.listener.core;

import io.github.alphabet.message.listener.core.anotations.DynamicListenerType;
import io.github.alphabet.message.listener.core.anotations.ListenerProcessorStrategy;
import io.github.alphabet.message.listener.core.anotations.MQListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.OrderComparator;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ObjectUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class MQAnnotationCoreBeanPostProcess implements InitializingBean, ApplicationContextAware, BeanPostProcessor {

    private final Map<DynamicListenerType, MQAnnotationCoreBeanPost> dynamicListenerTypeMQListenerAnnotationBeanPostProcessorMap = new ConcurrentHashMap<>();

    private ApplicationContext applicationContext;

    private AnnotationEnhancer enhancer;

    private List<MQAnnotationCoreBeanPost> mqCoreBeanPosts;

    public MQAnnotationCoreBeanPostProcess(List<MQAnnotationCoreBeanPost> mqCoreBeanPosts) {
        this.mqCoreBeanPosts = mqCoreBeanPosts;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

        // filter MQListener
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        Map<Method, Set<MQListener>> annotatedMethods = MethodIntrospector.selectMethods(
                targetClass,
                (MethodIntrospector.MetadataLookup<Set<MQListener>>) method -> {
                    Collection<MQListener> listenerMethods = this.findListenerAnnotations(method, MQListener.class);
                    return (!listenerMethods.isEmpty() ? new HashSet<>(listenerMethods) : null);
                }
        );

        // cycle dynamic process
        for (Map.Entry<Method, Set<MQListener>> entry : annotatedMethods.entrySet()) {
            Method method = entry.getKey();
            for (MQListener listener : entry.getValue()) {
                this.dynamicProcessListener(listener, method, bean, beanName);
            }
        }

        return bean;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        log.info("Initial loading annotation-bean-post has [{}] num", mqCoreBeanPosts.size());

        for (MQAnnotationCoreBeanPost mqAnnotationCoreBeanPost : mqCoreBeanPosts) {
            final ListenerProcessorStrategy strategy = AnnotatedElementUtils.findMergedAnnotation(mqAnnotationCoreBeanPost.getClass(), ListenerProcessorStrategy.class);
            dynamicListenerTypeMQListenerAnnotationBeanPostProcessorMap.put(strategy.strategy(), mqAnnotationCoreBeanPost);
            log.info("Initial loading annotation-bean-post strategy:【{}】", strategy.strategy());
        }

        this.buildEnhancer();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private void dynamicProcessListener(MQListener listener, Method method, Object bean, String beanName) {
        dynamicListenerTypeMQListenerAnnotationBeanPostProcessorMap.forEach((dynamicListenerType, easyListenerAnnotationBeanPostProcessor) -> {
            Boolean hasStrategy = easyListenerAnnotationBeanPostProcessor.hasStrategy(listener);
            if (hasStrategy) {
                easyListenerAnnotationBeanPostProcessor.processAssignMQListener(listener, method, bean, beanName);
            } else {
                if (!ObjectUtils.isEmpty(listener.topics())) {
                    easyListenerAnnotationBeanPostProcessor.processMQListener(listener, method, bean, beanName);
                }
            }
        });
    }

    private void buildEnhancer() {
        if (this.applicationContext != null) {
            Map<String, AnnotationEnhancer> enhancersMap =
                    this.applicationContext.getBeansOfType(AnnotationEnhancer.class, false, false);
            if (enhancersMap.size() > 0) {
                List<AnnotationEnhancer> enhancers = enhancersMap.values()
                        .stream()
                        .sorted(new OrderComparator())
                        .collect(Collectors.toList());
                this.enhancer = (attrs, element) -> {
                    Map<String, Object> newAttrs = attrs;
                    for (AnnotationEnhancer enh : enhancers) {
                        newAttrs = enh.apply(newAttrs, element);
                    }
                    return attrs;
                };
            }
        }
    }

    private <A extends Annotation> Collection<A> findListenerAnnotations(AnnotatedElement element, Class<A> annotationType) {
        Set<A> listeners = new HashSet<>();
        A ann = AnnotatedElementUtils.findMergedAnnotation(element, annotationType);
        if (ann != null) {
            ann = enhance(element, ann, annotationType);
            listeners.add(ann);
        }
        return listeners;
    }

    private <A extends Annotation> A enhance(AnnotatedElement element, A ann, Class<A> annotationType) {
        if (this.enhancer == null) {
            return ann;
        } else {
            return AnnotationUtils.synthesizeAnnotation(
                    this.enhancer.apply(AnnotationUtils.getAnnotationAttributes(ann), element), annotationType, null);
        }
    }

    /**
     * Post processes each set of annotation attributes.
     *
     * @since 2.7.2
     */
    public interface AnnotationEnhancer extends BiFunction<Map<String, Object>, AnnotatedElement, Map<String, Object>> {

    }
}
