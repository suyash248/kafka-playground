package com.soni.message.producers.impl;

import com.soni.config.Config;
import com.soni.message.producers.MessageProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MessageProducerImpl<K,V> implements MessageProducer<K,V> {
    Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaProducer<K, V> producer;

    public MessageProducerImpl() {
        Properties props = createProducerConfig();
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void pushMessage(String topic, K key, V message) {
        ProducerRecord<K, V> producerRecord;
        if (key == null)
            producerRecord = new ProducerRecord<>(topic, message);
        else
            producerRecord = new ProducerRecord<>(topic, key, message);
        producer.send(producerRecord,  (recordMetadata, e) -> {
            if (e != null) {
                logger.error("Couldn't send message", e);
            }
            logger.info("Sent:" + message + ", Partition: " + recordMetadata.partition() + ", Offset: "
                    + recordMetadata.offset());
        });
    }

    @Override
    public void shutdown() {
        producer.flush();
        producer.close();
    }

    private Properties createProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}
