package com.github.sibdevtools.service.kafka.client.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Entity(name = "kafka_client_service_bootstrap_group")
@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Table(schema = "kafka_client_service", name = "bootstrap_group")
public class BootstrapGroupEntity {
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Column(name = "code", nullable = false, unique = true)
    private String code;
    @Column(name = "name", nullable = false)
    private String name;
    @ElementCollection
    @CollectionTable(
            schema = "kafka_client_service",
            name = "bootstrap_group_services",
            joinColumns = @JoinColumn(name = "bootstrap_group_id")
    )
    @Column(name = "service", nullable = false)
    private List<String> bootstrapServices;
    @Column(name = "created_at", nullable = false)
    private ZonedDateTime createdAt;
    @Column(name = "modified_at", nullable = false)
    private ZonedDateTime modifiedAt;
}
