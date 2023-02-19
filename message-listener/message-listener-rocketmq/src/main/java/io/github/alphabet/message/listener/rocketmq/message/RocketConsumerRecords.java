package io.github.alphabet.message.listener.rocketmq.message;

import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import io.github.alphabet.message.listener.core.message.ConsumerRecords;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class RocketConsumerRecords implements ConsumerRecords<Object, String> {

    private final List<Message> records;

    public RocketConsumerRecords(List<Message> records) {
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
            messageList.add(new RocketConsumerRecord(null, new String(next.getBody()), next));
        }
        return messageList.iterator();
    }


}
