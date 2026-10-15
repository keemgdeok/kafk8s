package io.github.keemgdeok.kafk8s;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

class MinIsrTest {
    private static ConfigEntry entry(String value, ConfigEntry.ConfigSource source) {
        return new ConfigEntry("min.insync.replicas", value, source, false, false, List.of(), ConfigEntry.ConfigType.INT, "");
    }

    @Test void explicitTopicValueDoesNotNeedBrokerConfigPermission() {
        var value = MinIsr.resolve(entry("2", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG), List.of());
        assertEquals(2, value.count());
        assertNull(value.problem());
    }

    @Test void inheritedDefaultsMustAgree() {
        var broker = entry("2", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
        var value = MinIsr.resolve(broker, List.of(broker, broker, broker));
        assertEquals(2, value.count());
        assertTrue(value.source().startsWith("VERIFIED_BROKER_DEFAULTS"));
    }

    @Test void divergentBrokerDefaultsCannotProducePass() {
        var broker = entry("2", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
        var value = MinIsr.resolve(broker, List.of(broker, entry("3", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
        assertEquals("INCONSISTENT_EVIDENCE", value.problem());
        assertNull(value.count());
    }

    @Test void deniedBrokerConfigIsUnknown() {
        var broker = entry("2", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
        assertEquals("CONFIG_UNAVAILABLE", MinIsr.resolve(broker, Arrays.asList(broker, null)).problem());
    }

    @Test void invalidValuesCannotProducePass() {
        assertEquals("INCONSISTENT_EVIDENCE", MinIsr.resolve(entry("invalid", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG), List.of()).problem());
        assertEquals("INCONSISTENT_EVIDENCE", MinIsr.resolve(entry("0", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG), List.of()).problem());
        assertEquals("CONFIG_UNAVAILABLE", MinIsr.resolve(null, List.of()).problem());
    }
}
