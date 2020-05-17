package org.github.message.consumers.impl;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import org.github.config.Config;
import org.github.config.DeliverySemantics;
import org.github.message.consumers.MessageConsumer;
import org.github.message.consumers.listeners.RebalanceListener;
import org.github.message.consumers.processors.MessageHandler;
import org.github.message.consumers.processors.MessageProcessor;
import org.github.message.consumers.processors.ThreadQueue;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerImpl<K, V> implements MessageConsumer<K,V> {
    private final Logger logger = LoggerFactory.getLogger(MessageConsumerImpl.class);
    private final String topic;
    private final KafkaConsumer<K, V> consumer;
    private final ConcurrentMap<Integer, ThreadQueue<ConsumerRecord<K,V>>> partitionThreadQueue;
    private final int numberOfThreads;
    private final DeliverySemantics deliverySemantics;

    public MessageConsumerImpl(String topic, String groupId, DeliverySemantics deliverySemantics, int numberOfThreads) {
        Properties prop = createConsumerConfig(groupId);
        this.topic = topic;
        this.deliverySemantics = deliverySemantics;
        this.consumer = new KafkaConsumer<>(prop);

        List<PartitionInfo> partitionInfoList = this.consumer.partitionsFor(topic);
        this.numberOfThreads = (numberOfThreads > 0 && numberOfThreads <= partitionInfoList.size())
                ? numberOfThreads : partitionInfoList.size();

        this.partitionThreadQueue = new ConcurrentHashMap<>();//preparePartitionThreadQueue(partitionInfoList);
        this.consumer.subscribe(Collections.singletonList(topic),
                new RebalanceListener<>(partitionThreadQueue, numberOfThreads));
    }

    public MessageConsumerImpl(String topic, String groupId, DeliverySemantics deliverySemantics) {
        this(topic, groupId, deliverySemantics, -1);
    }

    @Override
    public void consume(MessageProcessor<ConsumerRecord<K,V>> messageProcessor) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(5000L));
                Set<TopicPartition> topicPartitions = consumer.assignment();
                System.out.println(topicPartitions);
                if(consumerRecords.count() > 0) logger.info("\nFound " + consumerRecords.count() + " message(s).");
                for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                    int partition = consumerRecord.partition();
                    ThreadQueue<ConsumerRecord<K,V>> threadQueue = getThreadQueueForPartition(partition);
                    logger.info("Partition: " + consumerRecord.partition() + " Thread(index: "
                            + threadQueue.getExecutorService().toString() + ") - queue size: "
                            + threadQueue.getQueue().size() + " processing " + consumerRecord.value());
                    threadQueue.getQueue().add(consumerRecord);
                }
                List<Future<List<ConsumerRecord<K, V>>>> futureList = handleMessages(messageProcessor);

                if(deliverySemantics == DeliverySemantics.ATLEAST_ONCE) {
                    List<Map<TopicPartition, OffsetAndMetadata>> committedOffsets = commitOffsets(futureList);
                } else if(deliverySemantics == DeliverySemantics.ATMOST_ONCE) {
                    consumer.commitSync();
                    if(consumerRecords.count() > 0)
                        logger.info("Committed offsets for current batch (size=" + consumerRecords.count() + " )");
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            logger.info("Closed consumer!");
        }
    }

    private List< Future<List<ConsumerRecord<K, V>>>> handleMessages(MessageProcessor<ConsumerRecord<K,V>> messageProcessor) {
        List< Future<List<ConsumerRecord<K, V>>>> futureList = new ArrayList<>();

        partitionThreadQueue.forEach((partition, threadQueue) -> {
            ConcurrentLinkedQueue<ConsumerRecord<K, V>> queueForCurrThread = threadQueue.getQueue();
            if(!queueForCurrThread.isEmpty()) {
                MessageHandler<ConsumerRecord<K,V>> messageHandler = new MessageHandler<ConsumerRecord<K,V>>(queueForCurrThread, messageProcessor);
                Future<List<ConsumerRecord<K, V>>> consumerRecordListFuture = threadQueue.getExecutorService().submit(messageHandler);
                futureList.add(consumerRecordListFuture);
            }
        });
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

    private ThreadQueue<ConsumerRecord<K,V>> getThreadQueueForPartition(int partition) {
        return partitionThreadQueue.get(partition);
    }

    private ConcurrentMap<Integer, ThreadQueue<ConsumerRecord<K,V>>> preparePartitionThreadQueue(List<PartitionInfo> partitionInfoList) {
        ConcurrentMap<Integer, ThreadQueue<ConsumerRecord<K,V>>> partitionThreadQueue = new ConcurrentHashMap<>();
        List<ThreadQueue<ConsumerRecord<K,V>>> threadQueues = new ArrayList<>();
        IntStream.range(0, numberOfThreads).forEach(i -> threadQueues.add(i, ThreadQueue.of(
                Executors.newSingleThreadExecutor(), new ConcurrentLinkedQueue<>()
        )));

        int threadIndex = 0;
        for(PartitionInfo partitionInfo: partitionInfoList) {
            partitionThreadQueue.put(partitionInfo.partition(), threadQueues.get(threadIndex));
            logger.info("Partition -> ThreadQueueIndex: p-" + partitionInfo.partition() + "->" + threadIndex);
            threadIndex = (threadIndex + 1) % this.numberOfThreads;
        }
        return partitionThreadQueue;
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