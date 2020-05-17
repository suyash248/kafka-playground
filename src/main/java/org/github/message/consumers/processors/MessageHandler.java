package org.github.message.consumers.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageHandler<V> implements Callable<List<V>> {
    Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final MessageProcessor<V> messageProcessor;
    private final ConcurrentLinkedQueue<V> queue;

    public MessageHandler(ConcurrentLinkedQueue<V> queue, MessageProcessor<V> messageProcessor) {
        this.queue = queue;
        this.messageProcessor = messageProcessor;
    }

    @Override
    public List<V> call() {
        logger.info("Handler - passing " + queue.size() + " message(s) to processor...");
        List<V> recordList = new ArrayList<>();
        while(!queue.isEmpty()) {
            recordList.add(queue.poll());
        }
        messageProcessor.process(recordList);
        return recordList;
    }
}