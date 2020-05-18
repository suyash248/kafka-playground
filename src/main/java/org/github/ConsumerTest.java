package org.github;

import org.github.config.DeliverySemantics;
import org.github.message.consumers.MessageConsumer;
import org.github.message.consumers.impl.MessageConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
        consumer.consume(consumerRecordList ->
            consumerRecordList.forEach(consumerRecord -> {
                if (consumerRecord.value().equals("Hi 50")) {
                    int i = 20/0;
                }
                String content = "Consumed: " + consumerRecord.value() + " Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset()
                        + ", By ThreadID: " + Thread.currentThread().getId();
                logger.info(content);
                try {
                    Files.write(filePath, Collections.singletonList(content), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
                } catch (IOException e) {
                    logger.error("Error while writing to file", e);
                }
            })
            ,((consumerRecordList, e) -> {
                List<String> failedConsumerRecords = consumerRecordList.stream().map(consumerRecord ->
                        consumerRecord.topic() + " " + consumerRecord.value() + " " + consumerRecord.partition()
                ).collect(Collectors.toList());
                logger.error("Error while processing a batch(size=" + failedConsumerRecords.size() + "): "
                        + failedConsumerRecords.toString(), e);
            })
        );
    }
}
