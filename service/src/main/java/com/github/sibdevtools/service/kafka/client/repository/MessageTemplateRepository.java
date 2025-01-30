package com.github.sibdevtools.service.kafka.client.repository;

import com.github.sibdevtools.service.kafka.client.entity.MessageTemplateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MessageTemplateRepository extends JpaRepository<MessageTemplateEntity, Long> {

    Optional<MessageTemplateEntity> findByCode(String code);

}
