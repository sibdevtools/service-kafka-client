package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.common.api.rs.StandardRs;
import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupDto;
import com.github.sibdevtools.service.kafka.client.api.dto.BootstrapGroupRsDto;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageDto;
import com.github.sibdevtools.service.kafka.client.api.dto.TopicDescriptionDto;
import com.github.sibdevtools.service.kafka.client.service.BootstrapGroupService;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
    public StandardRs create(
            @RequestBody BootstrapGroupDto rq
    ) {
        bootstrapGroupService.create(rq);
        return new StandardRs();
    }

    @PutMapping("/{id}")
    public StandardRs update(
            @RequestBody BootstrapGroupDto rq,
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        bootstrapGroupService.update(id, rq);
        return new StandardRs();
    }

    @GetMapping("/{id}")
    public StandardBodyRs<BootstrapGroupRsDto> get(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var rs = bootstrapGroupService.get(id);
        return new StandardBodyRs<>(rs);
    }

    @GetMapping("/byCode/{code}")
    public StandardBodyRs<BootstrapGroupRsDto> getByCode(
            @PathVariable("code") String code
    ) {
        var rs = bootstrapGroupService.getByCode(code);
        return new StandardBodyRs<>(rs);
    }

    @GetMapping("/{id}/ping")
    public StandardBodyRs<Boolean> ping(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var rs = bootstrapGroupService.ping(id);
        return new StandardBodyRs<>(rs);
    }

    @GetMapping("/{id}/topics")
    public StandardBodyRs<TreeSet<String>> getTopicNames(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var rs = bootstrapGroupService.getTopicNames(id)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)));
        return new StandardBodyRs<>(new TreeSet<>(rs));
    }

    @GetMapping("/{id}/{topic}/partitions")
    public StandardBodyRs<TopicDescriptionDto> getPartitions(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic
    ) {
        var id = Long.parseLong(rawId);
        var topicDescription = bootstrapGroupService.getTopicDescription(id, topic)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)));
        var dto = new TopicDescriptionDto(topicDescription);
        return new StandardBodyRs<>(dto);
    }

    @GetMapping("/{id}/{topic}/messages")
    public StandardBodyRs<ArrayList<MessageDto>> getMessages(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestParam(value = "maxMessages", required = false, defaultValue = "10") Integer rawMaxMessages,
            @RequestParam(value = "maxTimeout", required = false, defaultValue = "5000") Long rawMaxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var maxTimeout = rawMaxTimeout == null ? 5000 : Math.min(rawMaxTimeout, 60000);
        var body = bootstrapGroupService.getMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)))
                .stream()
                .map(MessageDto::new)
                .collect(Collectors.toCollection(ArrayList::new));
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/{topic}/lastMessages")
    public StandardBodyRs<ArrayList<MessageDto>> getLastNMessages(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestParam(value = "maxMessages", required = false, defaultValue = "10") Integer rawMaxMessages,
            @RequestParam(value = "maxTimeout", required = false, defaultValue = "5000") Long rawMaxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var maxTimeout = rawMaxTimeout == null ? 5000 : Math.min(rawMaxTimeout, 60000);
        var body = bootstrapGroupService.getLastNMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new RuntimeException("Bootstrap group '%d' not found".formatted(id)))
                .stream()
                .map(MessageDto::new)
                .collect(Collectors.toCollection(ArrayList::new));
        return new StandardBodyRs<>(body);
    }

}
