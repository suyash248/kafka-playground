package org.github.message.producers;

public interface MessageProducer<K, V> {
    void pushMessage(String topic, K key, V message);
    void shutdown();

}
