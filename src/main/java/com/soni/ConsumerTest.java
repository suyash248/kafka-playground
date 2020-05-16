package com.soni;

import com.soni.config.DeliverySemantics;
import com.soni.message.consumers.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTest {
    Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    public static void main(String[] args) {
        String topic = "top8";
        String groupId =  "group-1";//"g" + Math.random();
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.consume(topic, groupId);
    }

    public void consume(String topic, String groupId) {
        MessageConsumer<Void, String> consumer = new MessageConsumer<>(topic, groupId, DeliverySemantics.ATLEAST_ONCE, 4);
        consumer.consume(consumerRecordList -> {
            consumerRecordList.forEach(consumerRecord -> {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("Consumed: " + consumerRecord.value() + " Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset()
                        + ", By ThreadID: " + Thread.currentThread().getId());
            });
        });
    }
}
