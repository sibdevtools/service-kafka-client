package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.common.api.rs.StandardRs;
import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.api.dto.*;
import com.github.sibdevtools.service.kafka.client.api.rq.SendMessageRq;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.service.BootstrapGroupService;
import com.github.sibdevtools.service.kafka.client.service.MessageConsumerService;
import com.github.sibdevtools.service.kafka.client.service.MessagePublisherService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@RestController
@RequestMapping("/kafka-client-service/bootstrap-group")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaClientServiceController {
    private final BootstrapGroupService bootstrapGroupService;
    private final MessageConsumerService messageConsumerService;
    private final MessagePublisherService messagePublisherService;

    @GetMapping("/")
    public StandardBodyRs<ArrayList<BootstrapGroupRsDto>> getAll() {
        var body = bootstrapGroupService.getAll();
        return new StandardBodyRs<>(new ArrayList<>(body));
    }

    @PostMapping("/")
    public StandardBodyRs<Long> create(
            @RequestBody BootstrapGroupDto rq
    ) {
        var body = bootstrapGroupService.create(rq);
        return new StandardBodyRs<>(body);
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

    @DeleteMapping("/{id}")
    public StandardRs delete(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        bootstrapGroupService.delete(id);
        return new StandardRs();
    }

    @GetMapping("/{id}")
    public StandardBodyRs<BootstrapGroupRsDto> get(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = bootstrapGroupService.get(id);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/ping")
    public StandardBodyRs<Boolean> ping(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = bootstrapGroupService.ping(id);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/topics")
    public StandardBodyRs<TreeSet<String>> getTopicNames(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = bootstrapGroupService.getTopicNames(id)
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "TOPICS_NOT_FOUND", "Can't get topic names"));
        return new StandardBodyRs<>(new TreeSet<>(body));
    }

    @GetMapping("/{id}/{topic}/description")
    public StandardBodyRs<TopicDescriptionDto> getTopicDescription(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic
    ) {
        var id = Long.parseLong(rawId);
        var body = bootstrapGroupService.getTopicDescription(id, topic)
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "TOPIC_DESCRIPTION_NOT_FOUND", "Can't get topic description"));
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/{topic}/messages")
    public StandardBodyRs<ArrayList<MessageDto>> getMessages(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestParam(value = "maxMessages", required = false, defaultValue = "10") Integer rawMaxMessages,
            @RequestParam(value = "maxTimeout", required = false) Long maxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var body = messageConsumerService.getMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "READ_ERROR", "Can't get messages"))
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
            @RequestParam(value = "maxTimeout", required = false) Long maxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var body = messageConsumerService.getLastNMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "READ_ERROR", "Can't get last messages"))
                .stream()
                .map(MessageDto::new)
                .collect(Collectors.toCollection(ArrayList::new));
        return new StandardBodyRs<>(new ArrayList<>(body));
    }

    @PostMapping("/{id}/{topic}/message")
    public StandardBodyRs<RecordMetadataDto> sendMessage(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestBody SendMessageRq rq
    ) {
        var id = Long.parseLong(rawId);
        var body = messagePublisherService.sendMessage(
                        id,
                        topic,
                        rq.getPartition(),
                        rq.getTimestamp(),
                        rq.getKey(),
                        rq.getValue(),
                        rq.getHeaders(),
                        rq.getMaxTimeout()

                )
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "SEND_ERROR", "Can't send message"));
        return new StandardBodyRs<>(body);
    }

}
