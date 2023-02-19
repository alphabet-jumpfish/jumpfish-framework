package io.github.alphabet.message.listener.pulsar;

import io.github.alphabet.message.listener.core.support.Acknowledgment;
import io.github.alphabet.message.listener.pulsar.message.PulsarConsumerRecords;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class ConsumerBatchAcknowledgment implements Acknowledgment {


    protected Consumer consumer;
    protected PulsarConsumerRecords records;

    public ConsumerBatchAcknowledgment(Consumer consumer, PulsarConsumerRecords records) {
        this.consumer = consumer;
        this.records = records;
    }

    @Override
    public void acknowledge() {
        try {
            records.records().stream().forEach(param -> {
                try {
                    consumer.acknowledge(param);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
            log.error("ack 消费数据失败" + e.getMessage());
        }
    }
}
