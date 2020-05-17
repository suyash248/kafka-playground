package org.github.message.consumers.listeners;

import org.github.message.consumers.processors.ThreadQueue;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class RebalanceListener<K,V> implements ConsumerRebalanceListener {
    private final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    private final ConcurrentMap<Integer, ThreadQueue<ConsumerRecord<K,V>>> partitionThreadQueue;
    private final int numberOfThreads;

    public RebalanceListener(ConcurrentMap<Integer, ThreadQueue<ConsumerRecord<K,V>>> partitionThreadQueue, int numberOfThreads) {
        this.partitionThreadQueue = partitionThreadQueue;
        this.numberOfThreads = numberOfThreads;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
        revokePartitionThreadsOnRebalance(revokedPartitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        assignPartitionThreadsOnRebalance(assignedPartitions);
    }

    private void revokePartitionThreadsOnRebalance(Collection<TopicPartition> revokedTopicPartitions) {
        logger.info("Before revocation: " + partitionThreadQueue.toString());
        List<Integer> revokedPartitions = revokedTopicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList());
        logger.info("Partitions revoked: " + revokedPartitions.toString());
        Set<ThreadQueue<ConsumerRecord<K,V>>> revokeCandidates = new HashSet<>();
        revokedPartitions.forEach(partition -> revokeCandidates.add(partitionThreadQueue.remove(partition)));

        Set<ThreadQueue<ConsumerRecord<K,V>>> retainedCadidates = new HashSet<>(partitionThreadQueue.values());
        revokeCandidates.removeAll(retainedCadidates);

        revokeCandidates.forEach(revokeTq -> {
            logger.info("Revoking corresponding thread: " + revokeTq.toString());
            revokeTq.getExecutorService().shutdown();
            if (!revokeTq.getQueue().isEmpty()) {
                logger.warn("*********WARNING**********\nThread(" + revokeTq.toString() + ")'s queue contains "
                        + revokeTq.getQueue().size() + " message(s).");
                revokeTq.getQueue().forEach(consumerRecord -> System.out.println("##WARNING: record in queue -" + consumerRecord.value()) );
            }
        });
        logger.info("After revocation: " + partitionThreadQueue.toString());
    }

    private void assignPartitionThreadsOnRebalance(Collection<TopicPartition> assignedTopicPartitions) {
        logger.info("Before assignment: " + partitionThreadQueue.toString());
        List<Integer> assignedPartitions = assignedTopicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList());
        logger.info("Partitions assigned: " + assignedPartitions.toString());

        List<ThreadQueue<ConsumerRecord<K,V>>> distinctThreadQueues = new ArrayList<>(new HashSet<>(partitionThreadQueue.values()));
        int numDistinctQueues = distinctThreadQueues.size();
        while(numDistinctQueues < numberOfThreads) {
            distinctThreadQueues.add(ThreadQueue.of(Executors.newSingleThreadExecutor(), new ConcurrentLinkedQueue<>()));
            numDistinctQueues++;
        }

        int threadIndex = 0;
        for(int assignedPartition: assignedPartitions) {
            if(partitionThreadQueue.containsKey(assignedPartition)) {
                continue;
            }
            partitionThreadQueue.put(assignedPartition, distinctThreadQueues.get(threadIndex));
            logger.info("Partition -> ThreadQueueIndex: p-" + assignedPartition + "->" + threadIndex);
            threadIndex = (threadIndex + 1) % numberOfThreads;
        }
        logger.info("After assignment: " + partitionThreadQueue.toString());
    }
}
