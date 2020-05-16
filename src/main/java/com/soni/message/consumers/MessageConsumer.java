package com.soni.message.consumers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import com.soni.config.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer<K, V> {
    private final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    private final String topic;
    private final KafkaConsumer<K, V> consumer;
    private final Map<Integer, ConcurrentLinkedQueue<ConsumerRecord<K,V>>> queueMap;
    private final ExecutorService[] pools;
    private final int numberOfThreads;

    public MessageConsumer(String topic, String groupId, int numberOfThreads) {
        Properties prop = createConsumerConfig(groupId);
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(prop);
        this.queueMap = new HashMap<>();
        this.numberOfThreads = numberOfThreads;
        this.pools = new ExecutorService[numberOfThreads];

        for (int i = 0; i < pools.length; i++) {
            pools[i] = Executors.newSingleThreadExecutor();
            queueMap.put(i, new ConcurrentLinkedQueue<>());
        }
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void consume(MessageProcessor<K, V> messageProcessor) {
        try {
            while (true) {
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    logger.error("Error", e);
                }
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(5000L));
                if(consumerRecords.count() > 0) logger.info("\nFound " + consumerRecords.count() + " message(s).");
                for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                    int partition = consumerRecord.partition();
                    int hashIndex = partition % this.numberOfThreads;

                    ConcurrentLinkedQueue<ConsumerRecord<K, V>> queue = queueMap.get(hashIndex);
                    logger.info("Partition: " + consumerRecord.partition() + " Thread(index: " + hashIndex + ") - queue size: " + queue.size() + " processing " + consumerRecord.value());
                    queue.add(consumerRecord);
                }
                List< Future<List<ConsumerRecord<K, V>>>> futureList = handleMessages(messageProcessor);

//                List<Map<TopicPartition, OffsetAndMetadata>> committedOffsets = commitOffsets(futureList);
                consumer.commitSync();
                if(consumerRecords.count() > 0) logger.info("Committed offsets for current batch (size=" + consumerRecords.count() + " )");
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            logger.info("Closed consumer!");
        }
    }

    private List< Future<List<ConsumerRecord<K, V>>>> handleMessages(MessageProcessor<K, V> messageProcessor) {
        List< Future<List<ConsumerRecord<K, V>>>> futureList = new ArrayList<>();
        for (int i = 0; i < pools.length; i++) {
            ConcurrentLinkedQueue<ConsumerRecord<K, V>> queueForCurrThread = queueMap.get(i);
            if(queueForCurrThread.isEmpty()) {
                continue;
            }
            MessageHandler<K, V> messageHandler = new MessageHandler<>(queueForCurrThread, messageProcessor);
            Future<List<ConsumerRecord<K, V>>> consumerRecordListFuture = pools[i].submit(messageHandler);
            futureList.add(consumerRecordListFuture);
        }
        return futureList;
    }

    private List<Map<TopicPartition, OffsetAndMetadata>> commitOffsets(List<Future<List<ConsumerRecord<K, V>>>> futureList) {
        List<Map<TopicPartition, OffsetAndMetadata>> committedOffsets = new ArrayList<>();
        for (Future<List<ConsumerRecord<K, V>>> consumerRecordListFuture: futureList) {
            try {
                List<ConsumerRecord<K, V>> consumerRecordList = consumerRecordListFuture.get();
                logger.debug("Future resolved!");
                // Different partitions, different offsets. Find max offset for each partition and commit.
                Map<Integer, Long> partitionMaxOffsetMap = new HashMap<>();
                for (ConsumerRecord<K, V> consumerRecord : consumerRecordList) {
                    long currOffset = partitionMaxOffsetMap.getOrDefault(consumerRecord.partition(), -1L);
                    partitionMaxOffsetMap.put(consumerRecord.partition(), Math.max(currOffset, consumerRecord.offset()));
                    logger.debug("Partition: " + consumerRecord.partition() + " | Current offset: " + currOffset);
                }

                partitionMaxOffsetMap.forEach((partition, maxOffset) -> {
                    logger.debug("Committing topic: " + topic + " partition: " + partition + " offset: " + maxOffset);
                    Map<TopicPartition, OffsetAndMetadata> topicPartitionMaxOffset =
                            Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(maxOffset + 1));
                    consumer.commitSync(topicPartitionMaxOffset);
                    logger.info("Committed topic: " + topic + " partition: " + partition + " offset: " + maxOffset);
                    committedOffsets.add(topicPartitionMaxOffset);
                });
                logger.info("\n\n");
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Couldn't commit offsets", e);
            }
        }
        return committedOffsets;
    }

    private Properties createConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
