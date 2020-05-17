package com.soni;

import com.soni.config.DeliverySemantics;
import com.soni.message.consumers.MessageConsumer;
import com.soni.message.consumers.impl.MessageConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;

public class ConsumerTest {
    Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    public static void main(String[] args) {
        String topic = "top12";
        String groupId =  "group-1";//"g" + Math.random();
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.consume(topic, groupId);
    }

    public void consume(String topic, String groupId) {
        MessageConsumer<Void, String> consumer = new MessageConsumerImpl<>(topic, groupId, DeliverySemantics.ATLEAST_ONCE, 4);
        Path filePath = Paths.get("/home/suyash/IdeaProjects/kafka-playground/consumed.txt");
        consumer.consume(consumerRecordList -> {
            consumerRecordList.forEach(consumerRecord -> {
                String content = "Consumed: " + consumerRecord.value() + " Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset()
                        + ", By ThreadID: " + Thread.currentThread().getId();
                logger.info(content);
                try {
                    Files.write(filePath, Collections.singletonList(content), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
                } catch (IOException e) {
                    logger.error("Error while writing to file", e);
                }
            });
        });
    }
}
