package org.github.message.consumers;

import org.github.message.consumers.processors.ErrorHandler;
import org.github.message.consumers.processors.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface MessageConsumer<K,V> {
    void consume(MessageProcessor<List<ConsumerRecord<K,V>>> messageProcessor);
    void consume(MessageProcessor<List<ConsumerRecord<K,V>>> messageProcessor,
                 ErrorHandler<List<ConsumerRecord<K,V>>> errorHandler);
}
