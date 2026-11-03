package io.translab.tantor.server.service;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Set;

/** Kafka parses these byte limits as integers and returns their canonical decimal form. */
final class KafkaConfigValues {
    private KafkaConfigValues() {}
    private static final Set<String> INTEGER_BYTE_LIMITS = Set.of("message.max.bytes", "log.segment.bytes");

    static String canonical(String key, String value) {
        if (value == null || !INTEGER_BYTE_LIMITS.contains(key)) return value;
        try { return new BigInteger(value.trim()).toString(); }
        catch (NumberFormatException invalid) { return value; } // Kafka validation reports invalid input.
    }

    static boolean same(String key, Object first, Object second) {
        return Objects.equals(canonical(key, first == null ? null : first.toString()),
                canonical(key, second == null ? null : second.toString()));
    }
}
