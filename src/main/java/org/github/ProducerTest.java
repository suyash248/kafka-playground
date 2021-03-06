package org.github;

import org.github.message.producers.MessageProducer;
import org.github.message.producers.impl.MessageProducerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTest {
    Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    public static void main(String[] args) {
        String topic = "top12";
        ProducerTest producerTest = new ProducerTest();
        producerTest.produce(topic);
    }

    public void produce(String topic) {
        MessageProducer<Void, String> messageProducer = new MessageProducerImpl<>();
        for (int i = 0; i < 200; i++) {
            String message = "Hi " + i; // + " @ "  +  System.currentTimeMillis();
            messageProducer.pushMessage(topic, null, message);
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                logger.error("Error", e);
            }
        }
        messageProducer.shutdown();
    }
}
