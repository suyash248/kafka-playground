package org.github.message.consumers.impl;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import org.github.config.Config;
import org.github.config.DeliverySemantics;
import org.github.message.consumers.MessageConsumer;
import org.github.message.consumers.listeners.RebalanceListener;
import org.github.message.consumers.processors.ErrorHandler;
import org.github.message.consumers.processors.MessageProcessor;
import org.github.message.consumers.processors.ExecutorServiceQueue;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.github.message.producers.MessageProducer;
import org.github.message.producers.impl.MessageProducerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerImpl<K, V> implements MessageConsumer<K,V> {
    private final Logger logger = LoggerFactory.getLogger(MessageConsumerImpl.class);
    private final String topic;
    private final KafkaConsumer<K, V> consumer;
    private final ConcurrentMap<Integer, ExecutorServiceQueue<ConsumerRecord<K,V>>> partitionThreadQueue;
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
    }

    public MessageConsumerImpl(String topic, String groupId, DeliverySemantics deliverySemantics) {
        this(topic, groupId, deliverySemantics, -1);
    }

    @Override
    public void consume(MessageProcessor<List<ConsumerRecord<K,V>>> messageProcessor) {
        consume(messageProcessor, (e, records) -> {});
    }

    @Override
    public void consume(MessageProcessor<List<ConsumerRecord<K,V>>> messageProcessor,
                        ErrorHandler<List<ConsumerRecord<K,V>>> errorHandler) {
        try {
            this.consumer.subscribe(Collections.singletonList(topic),
                    new RebalanceListener<>(partitionThreadQueue, numberOfThreads));
            while (true) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(5000L));
                Set<TopicPartition> topicPartitions = consumer.assignment();
                System.out.println(topicPartitions);
                if(consumerRecords.count() > 0) logger.info("\nFound " + consumerRecords.count() + " message(s).");
                for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                    int partition = consumerRecord.partition();
                    ExecutorServiceQueue<ConsumerRecord<K,V>> executorServiceQueue = getThreadQueueForPartition(partition);
                    logger.info("Partition: " + consumerRecord.partition() + " Thread(index: "
                            + executorServiceQueue.getExecutorService().toString() + ") - queue size: "
                            + executorServiceQueue.getQueue().size() + " processing " + consumerRecord.value());
                    executorServiceQueue.getQueue().add(consumerRecord);
                }
                List<Future<List<ConsumerRecord<K, V>>>> futureList = handleMessages(messageProcessor, errorHandler);

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

    private List<Future<List<ConsumerRecord<K, V>>>> handleMessages(
            MessageProcessor<List<ConsumerRecord<K,V>>> messageProcessor,
            ErrorHandler<List<ConsumerRecord<K,V>>> errorHandler) {
        List<Future<List<ConsumerRecord<K, V>>>> futureList = new ArrayList<>();

        partitionThreadQueue.forEach((partition, executorServiceQueue) -> {
            ConcurrentLinkedQueue<ConsumerRecord<K, V>> queue = executorServiceQueue.getQueue();
            ExecutorService executorService = executorServiceQueue.getExecutorService();
            if(!queue.isEmpty()) {
                // Separate message handler for the records corresponding to a specific partition, so that we can
                // keep track of each partition's records via separate future.
                Future<List<ConsumerRecord<K, V>>> consumerRecordListFuture = executorService.submit(() -> {
                    logger.info("Handler - passing " + queue.size() + " message(s) to processor...");
                    List<ConsumerRecord<K,V>> recordList = new ArrayList<>();
                    while(!queue.isEmpty()) {
                        recordList.add(queue.poll());
                    }
                    try {
                        messageProcessor.process(recordList);
                    } catch (Exception e) {
                        // If there is any error while processing the batch of records.
                        logger.error("Error in processing...............");
                        new Thread(() -> errorHandler.onError(recordList, e)).start();
                    }
                    return recordList;
                });
                futureList.add(consumerRecordListFuture);
            }
        });
        return futureList;
    }

    private List<Map<TopicPartition, OffsetAndMetadata>> commitOffsets(List<Future<List<ConsumerRecord<K, V>>>> futureList) {
        List<Map<TopicPartition, OffsetAndMetadata>> committedOffsets = new ArrayList<>();
        for (Future<List<ConsumerRecord<K, V>>> consumerRecordListFuture: futureList) {
            logger.debug("Future resolved!");
            List<ConsumerRecord<K, V>> consumerRecordList = new ArrayList<>();
            try {
                consumerRecordList = consumerRecordListFuture.get();
            } catch (ExecutionException | CancellationException | InterruptedException e) {
                // TODO - Error handling should be done here...
                logger.error("Future failed: Couldn't commit offsets", e);
            }
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

        }
        return committedOffsets;
    }

    private ExecutorServiceQueue<ConsumerRecord<K,V>> getThreadQueueForPartition(int partition) {
        return partitionThreadQueue.get(partition);
    }

    private ConcurrentMap<Integer, ExecutorServiceQueue<ConsumerRecord<K,V>>> preparePartitionThreadQueue(List<PartitionInfo> partitionInfoList) {
        ConcurrentMap<Integer, ExecutorServiceQueue<ConsumerRecord<K,V>>> partitionThreadQueue = new ConcurrentHashMap<>();
        List<ExecutorServiceQueue<ConsumerRecord<K,V>>> executorServiceQueues = new ArrayList<>();
        IntStream.range(0, numberOfThreads).forEach(i -> executorServiceQueues.add(i, ExecutorServiceQueue.of(
                Executors.newSingleThreadExecutor(), new ConcurrentLinkedQueue<>()
        )));

        int threadIndex = 0;
        for(PartitionInfo partitionInfo: partitionInfoList) {
            partitionThreadQueue.put(partitionInfo.partition(), executorServiceQueues.get(threadIndex));
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