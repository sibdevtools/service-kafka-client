package com.github.sibdevtools.service.kafka.client.service;

import com.github.sibdevtools.service.kafka.client.api.dto.RecordMetadataDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Service
public class MessagePublisherService {
    private final BootstrapGroupService bootstrapGroupService;

    public MessagePublisherService(BootstrapGroupService bootstrapGroupService) {
        this.bootstrapGroupService = bootstrapGroupService;
    }

    /**
     * Send a message to Kafka topic.
     *
     * @param id         bootstrap group id
     * @param topic      topic to publish
     * @param partition  partition to publish
     * @param timestamp  message timestamp
     * @param key        message key
     * @param value      message value
     * @param headersMap message headers
     * @param maxTimeout max send timeout in milliseconds
     * @return data about sent message metadata or empty optional if sending failed
     */
    public Optional<RecordMetadataDto> sendMessage(
            long id,
            String topic,
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headersMap,
            Integer maxTimeout
    ) {
        var entity = bootstrapGroupService.get(id);
        maxTimeout = maxTimeout == null ? entity.getMaxTimeout() : maxTimeout;

        return sendMessage(
                entity.getBootstrapServers(),
                topic,
                partition,
                timestamp,
                key,
                value,
                headersMap,
                maxTimeout
        );
    }

    /**
     * Send a message to Kafka topic.
     *
     * @param bootstrapServers bootstrap servers line
     * @param topic            topic to publish
     * @param partition        partition to publish
     * @param timestamp        message timestamp
     * @param key              message key
     * @param value            message value
     * @param headersMap       message headers
     * @param maxTimeout       max send timeout in milliseconds
     * @return data about sent message metadata or empty optional if sending failed
     */
    public Optional<RecordMetadataDto> sendMessage(
            List<String> bootstrapServers,
            String topic,
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headersMap,
            int maxTimeout
    ) {
        var bootstrapServersLine = String.join(",", bootstrapServers);

        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersLine);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, maxTimeout);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, maxTimeout);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, maxTimeout);
        properties.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, maxTimeout);

        var headers = new RecordHeaders();
        if (headersMap != null) {
            for (var entry : headersMap.entrySet()) {
                headers.add(entry.getKey(), entry.getValue());
            }
        }

        try (var producer = new KafkaProducer<byte[], byte[]>(properties)) {
            var producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value, headers);
            var metadataFuture = producer.send(producerRecord);
            var recordMetadata = metadataFuture.get(maxTimeout, TimeUnit.MILLISECONDS);
            return Optional.ofNullable(recordMetadata)
                    .map(RecordMetadataDto::new);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't send message", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Can't send message", e);
            return Optional.empty();
        }
    }
}
