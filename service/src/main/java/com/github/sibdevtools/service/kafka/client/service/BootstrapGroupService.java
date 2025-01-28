package com.github.sibdevtools.service.kafka.client.service;

import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import com.github.sibdevtools.service.kafka.client.repository.BootstrapGroupRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
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

    public BootstrapGroupEntity get(long id) {
        return repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)));
    }

    public BootstrapGroupEntity getByCode(String code) {
        return repository.findByCode(code)
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
}
