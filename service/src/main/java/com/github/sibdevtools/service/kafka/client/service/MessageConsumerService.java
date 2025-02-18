package com.github.sibdevtools.service.kafka.client.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Service
public class MessageConsumerService {
    private final BootstrapGroupService bootstrapGroupService;

    public MessageConsumerService(BootstrapGroupService bootstrapGroupService) {
        this.bootstrapGroupService = bootstrapGroupService;
    }

    private static Properties getProperties(String bootstrapServers, int maxTimeout) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, maxTimeout);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, maxTimeout);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, maxTimeout);
        properties.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, maxTimeout);
        return properties;
    }

    private static void changeOffsets(int maxMessages, KafkaConsumer<byte[], byte[]> consumer) {
        var partitions = consumer.assignment();
        var beginningOffsets = consumer.beginningOffsets(partitions);
        var endOffsets = consumer.endOffsets(partitions);
        var offsetsPool = new HashMap<TopicPartition, Pair<Long, Long>>();
        for (var partition : partitions) {
            var beginOffset = beginningOffsets.get(partition);
            var endOffset = endOffsets.get(partition);
            if (beginOffset == null || endOffset == null || beginOffset.equals(endOffset)) {
                continue;
            }
            offsetsPool.put(partition, Pair.of(beginOffset, endOffset));
        }

        var totalAvailableMessages = offsetsPool.values().stream()
                .mapToLong(pair -> pair.getRight() - pair.getLeft())
                .sum();

        if (totalAvailableMessages <= maxMessages) {
            for (var entry : offsetsPool.entrySet()) {
                var offsets = entry.getValue();
                consumer.seek(entry.getKey(), offsets.getLeft());
            }
        } else {
            var offsets = getPartitionOffsets(maxMessages, offsetsPool);

            for (var entry : offsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
        }
    }

    private static HashMap<TopicPartition, Long> getPartitionOffsets(
            int maxMessages,
            Map<TopicPartition, Pair<Long, Long>> offsetsPool
    ) {
        var offsets = new HashMap<TopicPartition, Long>();
        long remainingMessages = maxMessages;

        var found = true;
        while (remainingMessages > 0 && found) {
            found = false;
            for (var entry : offsetsPool.entrySet()) {
                var partition = entry.getKey();
                var partitionOffsets = entry.getValue();
                var beginOffset = partitionOffsets.getLeft();
                var endOffset = partitionOffsets.getRight();
                var offset = offsets.getOrDefault(partition, endOffset);

                if (offset > beginOffset) {
                    offsets.put(partition, offset - 1);
                    remainingMessages -= 1;
                    found = true;
                    if (remainingMessages <= 0) {
                        break;
                    }
                }
            }
        }
        return offsets;
    }

    public Optional<List<ConsumerRecord<byte[], byte[]>>> getMessages(
            long id,
            String topic,
            int maxMessages,
            Integer maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = bootstrapGroupService.get(id);
        maxTimeout = maxTimeout == null ? entity.getMaxTimeout() : maxTimeout;
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = getProperties(bootstrapServers, maxTimeout);

        var messages = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            consumer.subscribe(List.of(topic));

            while (messages.size() < maxMessages && maxTimeout > 0) {
                var records = consumer.poll(Duration.ofMillis(maxTimeout));
                var currentTime = System.currentTimeMillis();
                maxTimeout -= Math.toIntExact(currentTime - timer);
                timer = currentTime;

                for (var message : records) {
                    messages.add(message);

                    if (messages.size() >= maxMessages) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        }
        return Optional.of(messages);
    }

    public Optional<List<ConsumerRecord<byte[], byte[]>>> getLastNMessages(
            long id,
            String topic,
            int maxMessages,
            Integer maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = bootstrapGroupService.get(id);
        maxTimeout = maxTimeout == null ? entity.getMaxTimeout() : maxTimeout;
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = getProperties(bootstrapServers, maxTimeout);

        var messages = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            consumer.subscribe(List.of(topic));

            consumer.poll(Duration.ofMillis(maxTimeout));
            var currentTime = System.currentTimeMillis();
            maxTimeout -= Math.toIntExact(currentTime - timer);
            timer = currentTime;

            changeOffsets(maxMessages, consumer);

            while (messages.size() < maxMessages && maxTimeout > 0) {
                var records = consumer.poll(Duration.ofMillis(maxTimeout));
                currentTime = System.currentTimeMillis();
                maxTimeout -= Math.toIntExact(currentTime - timer);
                timer = currentTime;
                for (var message : records) {
                    messages.add(message);

                    if (messages.size() >= maxMessages) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Can't read messages from Kafka", e);
            return Optional.empty();
        }
        return Optional.of(messages);
    }

}
