package com.github.sibdevtools.service.kafka.client.service;

import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupRsDto;
import com.github.sibdevtools.service.kafka.client.api.dto.TopicDescriptionDto;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import com.github.sibdevtools.service.kafka.client.exception.BootstrapGroupNotFoundException;
import com.github.sibdevtools.service.kafka.client.repository.BootstrapGroupRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

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

    public List<BootstrapGroupRsDto> getAll() {
        return repository.findAll(Sort.by("id"))
                .stream()
                .map(BootstrapGroupRsDto::new)
                .toList();
    }

    public long create(BootstrapGroupDto rq) {
        var entity = BootstrapGroupEntity.builder()
                .code(rq.getCode())
                .name(rq.getName())
                .maxTimeout(rq.getMaxTimeout())
                .bootstrapServers(rq.getBootstrapServers())
                .createdAt(ZonedDateTime.now())
                .modifiedAt(ZonedDateTime.now())
                .build();
        repository.save(entity);
        return entity.getId();
    }

    public BootstrapGroupRsDto get(long id) {
        return repository.findById(id)
                .map(BootstrapGroupRsDto::new)
                .orElseThrow(() -> new BootstrapGroupNotFoundException(id));
    }

    public BootstrapGroupRsDto getByCode(String code) {
        return repository.findByCode(code)
                .map(BootstrapGroupRsDto::new)
                .orElseThrow(() -> new BootstrapGroupNotFoundException(code));
    }

    public void update(long id, BootstrapGroupDto rq) {
        var entity = repository.findById(id)
                .orElseThrow(() -> new BootstrapGroupNotFoundException(id));

        entity.setCode(rq.getCode());
        entity.setName(rq.getName());
        entity.setMaxTimeout(rq.getMaxTimeout());
        entity.setBootstrapServers(rq.getBootstrapServers());
        entity.setModifiedAt(ZonedDateTime.now());

        repository.save(entity);
    }

    public void delete(long id) {
        repository.deleteById(id);
    }

    public boolean ping(long id) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());
        var maxTimeout = entity.getMaxTimeout();

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, maxTimeout);

        try (var adminClient = AdminClient.create(props)) {
            var topics = adminClient.listTopics();
            var topicsFuture = topics.names();
            topicsFuture.get(maxTimeout, TimeUnit.MILLISECONDS);
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

    public Optional<SortedSet<String>> getTopicNames(long id) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());
        var maxTimeout = entity.getMaxTimeout();

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, maxTimeout);

        try (var adminClient = AdminClient.create(props)) {
            var topics = adminClient.listTopics();
            var topicsFuture = topics.names();
            var names = topicsFuture.get(maxTimeout, TimeUnit.MILLISECONDS);
            return Optional.of(new TreeSet<>(names));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        }
    }

    public Optional<TopicDescriptionDto> getTopicDescription(long id, String topic) {
        var entity = get(id);
        var bootstrapServers = String.join(",", entity.getBootstrapServers());
        var maxTimeout = entity.getMaxTimeout();

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, maxTimeout);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, maxTimeout);

        try (var adminClient = AdminClient.create(props)) {
            var described = adminClient.describeTopics(Set.of(topic));
            var topicNameValues = described.topicNameValues();
            var topicDescriptionKafkaFuture = topicNameValues.get(topic);
            var topicDescription = topicDescriptionKafkaFuture.get(maxTimeout, TimeUnit.MILLISECONDS);
            return Optional.of(topicDescription)
                    .map(TopicDescriptionDto::new);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Can't ping bootstrap group", e);
            return Optional.empty();
        }
    }
}
