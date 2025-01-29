package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TopicDescriptionDto implements Serializable {
    private String name;
    private boolean internal;
    private List<TopicPartitionInfoDto> partitions;
    private Set<AclOperation> authorizedOperations;
    private String topicId;

    public TopicDescriptionDto(TopicDescription description) {
        this.name = description.name();
        this.internal = description.isInternal();
        this.partitions = Optional.ofNullable(description.partitions())
                .orElseGet(List::of)
                .stream()
                .map(TopicPartitionInfoDto::new)
                .toList();
        this.authorizedOperations = Optional.ofNullable(description.authorizedOperations())
                .map(Set::copyOf)
                .orElse(null);
        this.topicId = description.topicId().toString();
    }
}
