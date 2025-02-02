package com.github.sibdevtools.service.kafka.client.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Entity(name = "kafka_client_service_message_template")
@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Table(schema = "kafka_client_service", name = "message_template")
public class MessageTemplateEntity {
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Column(name = "code", nullable = false, unique = true)
    private String code;
    @Column(name = "name", nullable = false)
    private String name;
    @Column(name = "engine", nullable = false)
    private MessageEngine engine;
    @ElementCollection
    @CollectionTable(
            schema = "kafka_client_service",
            name = "message_template_header",
            joinColumns = @JoinColumn(name = "message_template_id")
    )
    @MapKeyColumn(name = "header_name")
    @Column(name = "header_value", nullable = false)
    private Map<String, String> headers;
    @Column(name = "template_storage_type", nullable = false)
    private String templateStorageType;
    @Column(name = "template_storage_id", nullable = false)
    private String templateStorageId;
    @Column(name = "schema_storage_type", nullable = false)
    private String schemaStorageType;
    @Column(name = "schema_storage_id", nullable = false)
    private String schemaStorageId;
    @Column(name = "created_at", nullable = false)
    private ZonedDateTime createdAt;
    @Column(name = "modified_at", nullable = false)
    private ZonedDateTime modifiedAt;
}
