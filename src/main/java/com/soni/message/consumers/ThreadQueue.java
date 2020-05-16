package com.soni.message.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public class ThreadQueue<K,V> {

    private final ExecutorService executorService;
    private final ConcurrentLinkedQueue<ConsumerRecord<K,V>> queue;

    public ThreadQueue(ExecutorService executorService, ConcurrentLinkedQueue<ConsumerRecord<K, V>> queue) {
        this.executorService = executorService;
        this.queue = queue;
    }

    public static <K,V> ThreadQueue<K,V> of(ExecutorService executorService, ConcurrentLinkedQueue<ConsumerRecord<K, V>> queue) {
        return new ThreadQueue<>(executorService, queue);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ConcurrentLinkedQueue<ConsumerRecord<K, V>> getQueue() {
        return queue;
    }
}
