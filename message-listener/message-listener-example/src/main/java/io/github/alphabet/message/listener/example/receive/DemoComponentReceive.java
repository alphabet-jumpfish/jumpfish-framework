package io.github.alphabet.message.listener.example.receive;


import io.github.alphabet.message.listener.core.anotations.MQListener;
import io.github.alphabet.message.listener.core.anotations.RocketMQListener;
import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import io.github.alphabet.message.listener.core.message.ConsumerRecords;
import io.github.alphabet.message.listener.core.support.Acknowledgment;
import io.github.alphabet.message.listener.rocketmq.RocketMqProducerClient;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class DemoComponentReceive {

    private final RocketMqProducerClient rocketMqProducerClient;

    public DemoComponentReceive(RocketMqProducerClient rocketMqProducerClient) {
        this.rocketMqProducerClient = rocketMqProducerClient;
    }


    @Scheduled(fixedRate = 2000)
    void rocketDemo() {
        try {
            rocketMqProducerClient.sendMessage("TEST", "rocketmq message wangjiawen is");
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    @MQListener(
            topics = "TEST", consumer = "TEST"
            // rocketMQListener= @RocketMQListener(topics = "TEST", subscriptionName = "TEST", concurrency = "1", consumeMessageBatchMaxSize = 100, autoCommitACK = true)
    )
    public void mbaRocketTopic(ConsumerRecords<String, String> consumerRecords, Acknowledgment acknowledgment) {

        acknowledgment.acknowledge();

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            final String payload = consumerRecord.payload();
            System.out.println("TEST message:{}" + payload);
        }
    }

}
