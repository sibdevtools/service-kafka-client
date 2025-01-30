package com.github.sibdevtools.service.kafka.client.task;

import com.github.sibdevtools.async.api.entity.AsyncTask;
import com.github.sibdevtools.async.api.rs.AsyncTaskProcessingResult;
import com.github.sibdevtools.async.api.rs.AsyncTaskProcessingResultBuilder;
import com.github.sibdevtools.async.api.service.AsyncTaskProcessor;
import com.github.sibdevtools.async.api.service.AsyncTaskProcessorMeta;
import com.github.sibdevtools.service.kafka.client.entity.MessageTemplateEntity;
import com.github.sibdevtools.service.kafka.client.repository.MessageTemplateRepository;
import com.github.sibdevtools.service.kafka.client.service.TemplateMessageService;
import com.github.sibdevtools.storage.api.service.StorageService;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Component
@AsyncTaskProcessorMeta(
        taskType = "kafka-client-service.delete-unused-file",
        taskVersions = "v1"
)
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class CleanUpRemovedMockFileProcessor implements AsyncTaskProcessor {
    private final StorageService storageService;
    private final MessageTemplateRepository messageTemplateRepository;

    @Nonnull
    @Override
    public AsyncTaskProcessingResult process(@Nonnull AsyncTask asyncTask) {
        var parameters = asyncTask.parameters();
        var templateId = Optional.of(parameters.get("templateId"))
                .map(Long::valueOf)
                .orElseThrow(() -> new IllegalArgumentException("Missing templateId parameter"));
        var schemaStorageId = parameters.get(TemplateMessageService.SCHEMA_STORAGE_ID);
        if (schemaStorageId != null) {
            var currentFileId = messageTemplateRepository.findById(templateId)
                    .map(MessageTemplateEntity::getSchemaStorageId)
                    .orElse(null);

            if (Objects.equals(currentFileId, schemaStorageId)) {
                log.warn("Template {} still use file {}", templateId, schemaStorageId);
                return AsyncTaskProcessingResultBuilder.createRetryResult(
                        ZonedDateTime.now()
                                .plusMinutes(1)
                );
            }

            try {
                var rs = storageService.delete(schemaStorageId);
                if (rs.isSuccess()) {
                    return AsyncTaskProcessingResultBuilder.createFinishResult();
                }
            } catch (Exception e) {
                log.error("Failed to delete file {}: {}", schemaStorageId, e.getMessage(), e);
            }
            return AsyncTaskProcessingResultBuilder.createRetryResult(
                    ZonedDateTime.now()
                            .plusMinutes(1)
            );
        }

        var templateStorageId = parameters.get(TemplateMessageService.TEMPLATE_STORAGE_ID);

        var currentFileId = messageTemplateRepository.findById(templateId)
                .map(MessageTemplateEntity::getTemplateStorageId)
                .orElse(null);

        if (Objects.equals(currentFileId, templateStorageId)) {
            log.warn("Template {} still use file {}", templateId, templateStorageId);
            return AsyncTaskProcessingResultBuilder.createRetryResult(
                    ZonedDateTime.now()
                            .plusMinutes(1)
            );
        }

        try {
            var rs = storageService.delete(templateStorageId);
            if (rs.isSuccess()) {
                return AsyncTaskProcessingResultBuilder.createFinishResult();
            }
        } catch (Exception e) {
            log.error("Failed to delete file {}: {}", templateStorageId, e.getMessage(), e);
        }
        return AsyncTaskProcessingResultBuilder.createRetryResult(
                ZonedDateTime.now()
                        .plusMinutes(1)
        );
    }
}
