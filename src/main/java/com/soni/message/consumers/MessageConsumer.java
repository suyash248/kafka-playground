package com.soni.message.consumers;

import com.soni.message.consumers.processors.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageConsumer<K,V> {
    void consume(MessageProcessor<ConsumerRecord<K,V>> messageProcessor);
}
