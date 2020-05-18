package org.github.message.consumers.processors;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

/**
 * Container to map each {@link ExecutorService} to a {@link ConcurrentLinkedQueue}
 * @param <V>
 */
public class ExecutorServiceQueue<V> {

    private final ExecutorService executorService;
    private final ConcurrentLinkedQueue<V> queue;

    public ExecutorServiceQueue(ExecutorService executorService, ConcurrentLinkedQueue<V> queue) {
        this.executorService = executorService;
        this.queue = queue;
    }

    public static <V> ExecutorServiceQueue<V> of(ExecutorService executorService, ConcurrentLinkedQueue<V> queue) {
        return new ExecutorServiceQueue<>(executorService, queue);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ConcurrentLinkedQueue<V> getQueue() {
        return queue;
    }

    @Override
    public String toString() {
        return "ThreadQueue{" + "executorService=" + executorService + ", queue=" + queue + "}";
    }
}
