package com.github.sibdevtools.service.kafka.client.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.async.api.rq.CreateAsyncTaskRq;
import com.github.sibdevtools.async.api.service.AsyncTaskService;
import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateDto;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateRsDto;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateShortRsDto;
import com.github.sibdevtools.service.kafka.client.api.dto.RecordMetadataDto;
import com.github.sibdevtools.service.kafka.client.api.rq.SendTemplateMessageRq;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.entity.MessageTemplateEntity;
import com.github.sibdevtools.service.kafka.client.exception.MessageTemplateNotFoundException;
import com.github.sibdevtools.service.kafka.client.repository.MessageTemplateRepository;
import com.github.sibdevtools.service.kafka.client.template.TemplateMessageEngineFacade;
import com.github.sibdevtools.storage.api.rq.SaveFileRq;
import com.github.sibdevtools.storage.api.service.StorageService;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class TemplateMessageService {

    public static final String SCHEMA_STORAGE_ID = "schemaStorageId";
    public static final String TEMPLATE_STORAGE_ID = "templateStorageId";

    private final AsyncTaskService asyncTaskService;
    private final StorageService storageService;
    private final MessageTemplateRepository repository;
    private final MessagePublisherService messagePublisherService;
    private final TemplateMessageEngineFacade templateMessageEngineFacade;
    private final Base64.Decoder decoder = Base64.getDecoder();

    @Autowired
    @Qualifier("kafkaClientServiceObjectMapper")
    private ObjectMapper objectMapper;
    @Value("${kafka.client.service.props.bucket.code}")
    private String bucketCode;

    public List<MessageTemplateShortRsDto> getAll() {
        return repository.findAll()
                .stream()
                .map(MessageTemplateShortRsDto::new)
                .toList();
    }

    public long create(MessageTemplateDto rq) {
        var schemaBytes = serializeSchema(rq);

        var schemaFileId = saveFile(rq, schemaBytes);
        var templateFileId = saveFile(rq, rq.getTemplate());

        var entity = MessageTemplateEntity.builder()
                .code(rq.getCode())
                .name(rq.getName())
                .headers(rq.getHeaders())
                .engine(rq.getEngine())
                .schemaStorageType("LOCAL")
                .schemaStorageId(schemaFileId)
                .templateStorageType("LOCAL")
                .templateStorageId(templateFileId)
                .createdAt(ZonedDateTime.now())
                .modifiedAt(ZonedDateTime.now())
                .build();
        repository.save(entity);
        return entity.getId();
    }

    private byte[] serializeSchema(MessageTemplateDto rq) {
        var schema = rq.getSchema();
        byte[] schemaBytes;
        try {
            schemaBytes = objectMapper.writeValueAsBytes(schema);
        } catch (JsonProcessingException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "SERIALIZATION_ERROR", e.getMessage(), e);
        }
        return schemaBytes;
    }

    private String saveFile(MessageTemplateDto rq, byte[] schemaBytes) {
        var saveSchemaRq = SaveFileRq.builder()
                .bucket(bucketCode)
                .name(rq.getName())
                .meta(Map.ofEntries(
                        Map.entry("code", rq.getCode()),
                        Map.entry("engine", rq.getEngine().name())
                ))
                .data(schemaBytes)
                .build();
        var saveSchemaRs = storageService.save(saveSchemaRq);
        return saveSchemaRs.getBody();
    }

    public MessageTemplateRsDto get(long id) {
        return repository.findById(id)
                .map(it -> new MessageTemplateRsDto(it, readSchema(it), readTemplate(it)))
                .orElseThrow(() -> new MessageTemplateNotFoundException(id));
    }

    private Map<String, Object> readSchema(MessageTemplateEntity it) {
        var fileRs = storageService.get(it.getSchemaStorageId());
        if (!fileRs.isSuccess()) {
            throw new ServiceException(Constant.ERROR_SOURCE, "GET_FILE_ERROR", "Can't read schema file");
        }
        var schemaFileRsBody = fileRs.getBody();
        var rsBodyData = schemaFileRsBody.getData();
        try {
            return objectMapper.readValue(rsBodyData, Map.class);
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "PARSE_FILE_ERROR", "Can't parse schema file");
        }
    }

    private byte[] readTemplate(MessageTemplateEntity it) {
        var fileRs = storageService.get(it.getTemplateStorageId());
        if (!fileRs.isSuccess()) {
            throw new ServiceException(Constant.ERROR_SOURCE, "GET_FILE_ERROR", "Can't read template file");
        }
        var schemaFileRsBody = fileRs.getBody();
        return schemaFileRsBody.getData();
    }

    public MessageTemplateRsDto getByCode(String code) {
        return repository.findByCode(code)
                .map(it -> new MessageTemplateRsDto(it, readSchema(it), readTemplate(it)))
                .orElseThrow(() -> new MessageTemplateNotFoundException(code));
    }

    public void update(long id, MessageTemplateDto rq) {
        var entity = repository.findById(id)
                .orElseThrow(() -> new MessageTemplateNotFoundException(id));

        var template = rq.getTemplate();
        updateTemplateIfChanged(id, rq, entity, template);
        var schemaBytes = serializeSchema(rq);
        updateSchemaIfChanged(id, rq, entity, schemaBytes);

        entity.setCode(rq.getCode());
        entity.setName(rq.getName());
        entity.setHeaders(rq.getHeaders());
        entity.setEngine(rq.getEngine());
        entity.setModifiedAt(ZonedDateTime.now());

        repository.save(entity);
    }

    public void delete(long id) {
        var entity = repository.findById(id)
                .orElse(null);

        if (entity == null) {
            return;
        }

        registerCleanUpTask(id, SCHEMA_STORAGE_ID, entity.getSchemaStorageId());
        registerCleanUpTask(id, TEMPLATE_STORAGE_ID, entity.getTemplateStorageId());

        repository.delete(entity);
    }

    public Optional<RecordMetadataDto> send(
            long id,
            SendTemplateMessageRq rq
    ) {
        var entity = repository.findById(id)
                .orElseThrow(() -> new MessageTemplateNotFoundException(id));

        var rawSchema = readSchema(entity);

        var schemaNode = objectMapper.convertValue(rawSchema, JsonNode.class);

        var schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        var schema = schemaFactory.getSchema(schemaNode);

        var input = rq.getInput();
        var dataNode = objectMapper.convertValue(input, JsonNode.class);

        var validationResult = schema.validate(dataNode);

        if (!validationResult.isEmpty()) {
            throw new ServiceException(
                    Constant.ERROR_SOURCE,
                    "INVALID_INPUT",
                    validationResult.stream()
                            .map(ValidationMessage::getMessage)
                            .collect(Collectors.joining(","))
            );
        }

        var template = readTemplate(entity);

        var headers = new LinkedHashMap<String, byte[]>();
        var entityHeaders = entity.getHeaders();
        if (entityHeaders != null) {
            for (var entry : entityHeaders.entrySet()) {
                var base64Value = entry.getValue();
                var rawValue = decoder.decode(base64Value);
                headers.put(entry.getKey(), rawValue);
            }
        }
        var rqHeaders = rq.getHeaders();
        if (rqHeaders != null) {
            headers.putAll(rqHeaders);
        }

        var renderedMessage = templateMessageEngineFacade.render(
                entity.getEngine(),
                rq.getPartition(),
                rq.getTimestamp(),
                rq.getKey(),
                template,
                input,
                headers
        );

        return messagePublisherService.sendMessage(
                rq.getBootstrapGroupId(),
                rq.getTopic(),
                renderedMessage.getPartition(),
                renderedMessage.getTimestamp(),
                renderedMessage.getKey(),
                renderedMessage.getValue(),
                renderedMessage.getHeaders(),
                rq.getMaxTimeout()
        );
    }

    private void updateTemplateIfChanged(
            long id,
            MessageTemplateDto rq,
            MessageTemplateEntity entity,
            byte[] template
    ) {
        var templateFile = storageService.get(entity.getTemplateStorageId());
        if (templateFile.isSuccess()) {
            var templateFileBody = templateFile.getBody();
            var templateFileBodyData = templateFileBody.getData();
            if (!Arrays.equals(templateFileBodyData, template)) {
                var templateId = saveFile(rq, template);
                entity.setTemplateStorageId(templateId);
                registerCleanUpTask(id, TEMPLATE_STORAGE_ID, entity.getTemplateStorageId());
            }
        } else {
            var templateId = saveFile(rq, template);
            entity.setTemplateStorageId(templateId);
            registerCleanUpTask(id, TEMPLATE_STORAGE_ID, entity.getTemplateStorageId());
        }
    }

    private void updateSchemaIfChanged(
            long id,
            MessageTemplateDto rq,
            MessageTemplateEntity entity,
            byte[] schemaBytes
    ) {
        var schemaFile = storageService.get(entity.getSchemaStorageId());
        if (schemaFile.isSuccess()) {
            var schemaFileBody = schemaFile.getBody();
            var schemaFileBodyData = schemaFileBody.getData();
            if (!Arrays.equals(schemaFileBodyData, schemaBytes)) {
                var schemaId = saveFile(rq, schemaBytes);
                entity.setSchemaStorageId(schemaId);
                registerCleanUpTask(id, SCHEMA_STORAGE_ID, entity.getSchemaStorageId());
            }
        } else {
            var schemaId = saveFile(rq, schemaBytes);
            entity.setSchemaStorageId(schemaId);
            registerCleanUpTask(id, SCHEMA_STORAGE_ID, entity.getSchemaStorageId());
        }
    }

    private void registerCleanUpTask(long templateId, String storageType, String storageId) {
        var cleanUpTaskRs = asyncTaskService.registerTask(
                CreateAsyncTaskRq.builder()
                        .uid(UUID.randomUUID().toString())
                        .type("kafka-client-service.delete-unused-file")
                        .version("v1")
                        .scheduledStartTime(ZonedDateTime.now().plusSeconds(10))
                        .parameters(Map.ofEntries(
                                Map.entry("templateId", Long.toString(templateId)),
                                Map.entry(storageType, storageId)
                        ))
                        .build()
        );
        if (!cleanUpTaskRs.isSuccess() || !cleanUpTaskRs.getBody()) {
            throw new ServiceException(Constant.ERROR_SOURCE, "ASYNC_SERVICE_ERROR", "Failed to schedule cleanup task");
        }
    }
}
