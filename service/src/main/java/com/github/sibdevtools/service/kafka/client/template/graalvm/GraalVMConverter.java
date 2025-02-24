package com.github.sibdevtools.service.kafka.client.template.graalvm;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.*;

/**
 * @author sibmaks
 * @since 0.0.15
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GraalVMConverter {

    public static Serializable toSerializable(Object value) {
        if (value == null) return null;
        switch (value) {
            case Serializable serializable -> {
                return serializable;
            }
            case Map<?, ?> map -> {
                return new LinkedHashMap<>(map);
            }
            case Set<?> set -> {
                return new HashSet<>(set);
            }
            case Collection<?> collection -> {
                return new ArrayList<>(collection);
            }
            default -> {
                return null;
            }
        }
    }

    public static Map<String, Map<String, Serializable>> convertSections(Map<String, Map<String, ?>> sections) {
        if (sections == null) {
            return Collections.emptyMap();
        }
        var serialized = new LinkedHashMap<String, Map<String, Serializable>>();

        for (var entry : sections.entrySet()) {
            var section = new LinkedHashMap<String, Serializable>();
            serialized.put(entry.getKey(), section);

            var sectionAttributes = entry.getValue();
            if (sectionAttributes == null || sectionAttributes.isEmpty()) {
                continue;
            }

            for (var attributeEntry : sectionAttributes.entrySet()) {
                var key = attributeEntry.getKey();
                var value = attributeEntry.getValue();
                var serializedValue = toSerializable(value);
                if (serializedValue == null) {
                    log.warn("Skipped unsupported section attribute '{}', type: {}", key, value.getClass().getName());
                    continue;
                }
                section.put(key, serializedValue);
            }
        }

        return serialized;
    }

}
