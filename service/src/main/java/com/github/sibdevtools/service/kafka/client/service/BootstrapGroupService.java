package com.github.sibdevtools.service.kafka.client.service;

import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import com.github.sibdevtools.service.kafka.client.repository.BootstrapGroupRepository;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;

/**
 * @author sibmaks
 * @since 0.0.1
 */
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
                .bootstrapServices(rq.getBootstrapServices())
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
        entity.setBootstrapServices(rq.getBootstrapServices());
        entity.setModifiedAt(ZonedDateTime.now());

        repository.save(entity);
    }
}
