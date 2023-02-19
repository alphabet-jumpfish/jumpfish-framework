package io.github.alphabet.message.listener.pulsar.message;

import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import io.github.alphabet.message.listener.core.message.ConsumerRecords;
import org.apache.pulsar.client.api.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class PulsarConsumerRecords implements ConsumerRecords<Object, String> {

    private final List<Message> records;

    public PulsarConsumerRecords(List<Message> records) {
        this.records = records;
    }

    public List<Message> records() {
        return records;
    }


    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<ConsumerRecord<Object, String>> iterator() {

        final Iterator<Message> iterator = records.iterator();

        List<ConsumerRecord<Object,String>> messageList=new ArrayList<>();

        while (iterator.hasNext()) {
            final Message next = iterator.next();
            messageList.add(new PulsarConsumerRecord(null, new String(next.getData()), next));
        }
        return messageList.iterator();
    }


}
