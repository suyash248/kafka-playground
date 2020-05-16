package com.soni.message.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageHandler<K, V> implements Callable<List<ConsumerRecord<K, V>>> {
    Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final MessageProcessor<K, V> messageProcessor;
    private final ConcurrentLinkedQueue<ConsumerRecord<K,V>> queue;

    public MessageHandler(ConcurrentLinkedQueue<ConsumerRecord<K,V>> queue, MessageProcessor<K, V> messageProcessor) {
        this.queue = queue;
        this.messageProcessor = messageProcessor;
    }

    @Override
    public List<ConsumerRecord<K,V>> call() {
        logger.info("Handler - passing " + queue.size() + " message(s) to processor...");
        List<ConsumerRecord<K,V>> consumerRecordList = new ArrayList<>();
        while(!queue.isEmpty()) {
            ConsumerRecord<K, V> consumerRecord = queue.poll();
            consumerRecordList.add(consumerRecord);
        }
        messageProcessor.process(consumerRecordList);
        return consumerRecordList;
    }
}