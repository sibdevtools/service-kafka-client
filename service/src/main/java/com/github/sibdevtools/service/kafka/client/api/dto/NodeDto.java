package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.common.Node;

import java.io.Serializable;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class NodeDto implements Serializable {
    private int id;
    private String idString;
    private String host;
    private int port;
    private String rack;

    public NodeDto(Node node) {
        this.id = node.id();
        this.idString = node.idString();
        this.host = node.host();
        this.port = node.port();
        this.rack = node.rack();
    }
}
