package com.soni.message.producers;

import com.soni.ConsumerTest;
import com.soni.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MessageProducer<K, V> {
    Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaProducer<K, V> producer;

    public MessageProducer() {
        Properties props = createProducerConfig();
        this.producer = new KafkaProducer<>(props);
    }

    private Properties createProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

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

    public void shutdown() {
        producer.flush();
        producer.close();
    }

}
