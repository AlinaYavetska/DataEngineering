package ua.edu.de.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonSerde<T> implements Serde<T> {
    private static final ObjectMapper M = new ObjectMapper();
    private final Class<T> type;

    public JsonSerde(Class<T> type) { this.type = type; }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try { return M.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try { return M.readValue(new String(bytes, StandardCharsets.UTF_8), type); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
    }
}
