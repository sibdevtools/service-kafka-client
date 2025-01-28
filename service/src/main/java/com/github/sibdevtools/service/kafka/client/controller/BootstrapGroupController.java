package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import com.github.sibdevtools.service.kafka.client.service.BootstrapGroupService;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@RestController
@RequestMapping("/bootstrap-group")
public class BootstrapGroupController {
    private final BootstrapGroupService bootstrapGroupService;

    public BootstrapGroupController(BootstrapGroupService bootstrapGroupService) {
        this.bootstrapGroupService = bootstrapGroupService;
    }

    @PostMapping("/")
    public void create(
            @RequestBody BootstrapGroupDto rq
    ) {
        bootstrapGroupService.create(rq);
    }

    @PutMapping("/{id}")
    public void update(
            @RequestBody BootstrapGroupDto rq,
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        bootstrapGroupService.update(id, rq);
    }

    @GetMapping("/{id}")
    public BootstrapGroupEntity get(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        return bootstrapGroupService.get(id);
    }

    @GetMapping("/byCode/{code}")
    public BootstrapGroupEntity getByCode(
            @PathVariable("code") String code
    ) {
        return bootstrapGroupService.getByCode(code);
    }

    @GetMapping("/{id}/ping")
    public boolean ping(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        return bootstrapGroupService.ping(id);
    }

    @GetMapping("/{id}/topics")
    public Set<String> getTopicNames(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        return bootstrapGroupService.getTopicNames(id)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)));
    }

}
