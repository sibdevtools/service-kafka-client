package com.github.sibdevtools.service.kafka.client.repository;

import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface BootstrapGroupRepository extends JpaRepository<BootstrapGroupEntity, Long> {

    Optional<BootstrapGroupEntity> findByCode(String code);

}
