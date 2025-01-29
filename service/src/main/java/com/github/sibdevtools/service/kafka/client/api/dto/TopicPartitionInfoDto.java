package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.Serializable;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionInfoDto implements Serializable {
    private int partition;
    private NodeDto leader;
    private List<NodeDto> replicas;

    public TopicPartitionInfoDto(TopicPartitionInfo info) {
        this.partition = info.partition();
        this.leader = new NodeDto(info.leader());
        this.replicas = info.replicas().stream()
                .map(NodeDto::new)
                .toList();
    }
}
