package com.github.sibdevtools.service.kafka.client.service;

import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupRsDto;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import com.github.sibdevtools.service.kafka.client.repository.BootstrapGroupRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Service
public class BootstrapGroupService {
    private final BootstrapGroupRepository repository;

    public BootstrapGroupService(BootstrapGroupRepository repository) {
        this.repository = repository;
    }

    public void create(BootstrapGroupDto rq) {
        var entity = BootstrapGroupEntity.builder()
                .code(rq.getCode())
                .name(rq.getName())
                .maxTimeout(rq.getMaxTimeout())
                .bootstrapServers(rq.getBootstrapServers())
                .createdAt(ZonedDateTime.now())
                .modifiedAt(ZonedDateTime.now())
                .build();
        repository.save(entity);
    }

    public BootstrapGroupRsDto get(long id) {
        return repository.findById(id)
                .map(BootstrapGroupRsDto::new)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)));
    }

    public BootstrapGroupRsDto getByCode(String code) {
        return repository.findByCode(code)
                .map(BootstrapGroupRsDto::new)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%s' not found".formatted(code)));
    }

    public void update(long id, BootstrapGroupDto rq) {
        var entity = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Bootstrap group not found"));

        entity.setCode(rq.getCode());
        entity.setName(rq.getName());
        entity.setMaxTimeout(rq.getMaxTimeout());
        entity.setBootstrapServers(rq.getBootstrapServers());
        entity.setModifiedAt(ZonedDateTime.now());

        repository.save(entity);
    }

    public boolean ping(long id) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (var adminClient = AdminClient.create(props)) {
            var topics = adminClient.listTopics();
            var topicsFuture = topics.names();
            topicsFuture.get(entity.getMaxTimeout(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't ping bootstrap group", e);
            return false;
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return false;
        }
    }

    public Optional<Set<String>> getTopicNames(long id) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (var adminClient = AdminClient.create(props)) {
            var topics = adminClient.listTopics();
            var topicsFuture = topics.names();
            var names = topicsFuture.get(entity.getMaxTimeout(), TimeUnit.MILLISECONDS);
            return Optional.of(names);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        }
    }

    public Optional<TopicDescription> getTopicDescription(long id, String topic) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (var adminClient = AdminClient.create(props)) {
            var described = adminClient.describeTopics(Set.of(topic));
            var topicNameValues = described.topicNameValues();
            var topicDescriptionKafkaFuture = topicNameValues.get(topic);
            var topicDescription = topicDescriptionKafkaFuture.get(entity.getMaxTimeout(), TimeUnit.MILLISECONDS);
            return Optional.of(topicDescription);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        }
    }

    public Optional<List<ConsumerRecord<byte[], byte[]>>> getMessages(
            long id,
            String topic,
            int maxMessages,
            long maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
            long maxTimeout
    ) {
        var timer = System.currentTimeMillis();
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

    public Optional<RecordMetadata> sendMessage(
            long id,
            String topic,
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headersMap,
            long maxTimeout
    ) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());

        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-client-service" + UUID.randomUUID());

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
            return Optional.ofNullable(recordMetadata);
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
