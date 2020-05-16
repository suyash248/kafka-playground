package com.soni.message.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

@FunctionalInterface
public interface MessageProcessor<K, V> {
    void process(List<ConsumerRecord<K, V>> consumerRecordList);
}
