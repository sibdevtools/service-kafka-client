package com.github.sibdevtools.service.kafka.client.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    public Optional<List<ConsumerRecord<byte[], byte[]>>> getMessages(
            long id,
            String topic,
            int maxMessages,
            Long maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = bootstrapGroupService.get(id);
        maxTimeout = maxTimeout == null ? entity.getMaxTimeout() : maxTimeout;
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = getProperties(bootstrapServers);

        var messages = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            consumer.subscribe(List.of(topic));

            while (messages.size() < maxMessages && maxTimeout > 0) {
                var records = consumer.poll(Duration.ofMillis(maxTimeout));
                var currentTime = System.currentTimeMillis();
                maxTimeout -= currentTime - timer;
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
            Long maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = bootstrapGroupService.get(id);
        maxTimeout = maxTimeout == null ? entity.getMaxTimeout() : maxTimeout;
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = getProperties(bootstrapServers);

        var messages = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            consumer.subscribe(List.of(topic));

            consumer.poll(Duration.ofMillis(maxTimeout));
            var currentTime = System.currentTimeMillis();
            maxTimeout -= currentTime - timer;
            timer = currentTime;

            var partitions = consumer.assignment();
            consumer.seekToEnd(partitions);
            var endOffset = partitions.stream()
                    .mapToLong(consumer::position)
                    .sum();

            var startOffset = Math.max(0, endOffset - maxMessages);

            partitions.forEach(partition -> consumer.seek(partition, startOffset));

            while (messages.size() < maxMessages && maxTimeout > 0) {
                var records = consumer.poll(Duration.ofMillis(maxTimeout));
                currentTime = System.currentTimeMillis();
                maxTimeout -= currentTime - timer;
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

    private static Properties getProperties(String bootstrapServers) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

}
