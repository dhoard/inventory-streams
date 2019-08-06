package com.github.dhoard.kafka.streams;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class InventoryValue {

    long value;

    public InventoryValue() {
        // DO NOTHING
    }

    public InventoryValue(long value) {
        this.value = value;
    }

    public static class JSONSerializer extends StdSerializer<InventoryValue> {

        public JSONSerializer() {
            this(null);
        }

        public JSONSerializer(Class<InventoryValue> t) {
            super(t);
        }

        @Override
        public void serialize(
            InventoryValue value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {

            jgen.writeStartObject();
            jgen.writeNumberField("value", value.value);
            jgen.writeEndObject();
        }
    }

    public static class JSONDeserializer extends StdDeserializer<InventoryValue> {

        public JSONDeserializer() {
            this(null);
        }

        public JSONDeserializer(Class<InventoryValue> t) {
            super(t);
        }

        @Override
        public InventoryValue deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
            JsonNode node = jp.getCodec().readTree(jp);
            long value = node.get("value").asLong(0);

            return new InventoryValue(value);
        }
    }

    public static class JSONSerde implements Serializer<InventoryValue>, Deserializer<InventoryValue>, Serde<InventoryValue> {

        private static ObjectMapper objectMapper = new ObjectMapper();

        static {
            SimpleModule simpleModule = new SimpleModule();
            simpleModule.addDeserializer(InventoryValue.class, new JSONDeserializer());
            simpleModule.addSerializer(InventoryValue.class, new JSONSerializer());

            objectMapper.registerModule(simpleModule);
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public InventoryValue deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.readValue(data, InventoryValue.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final InventoryValue data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<InventoryValue> serializer() {
            return this;
        }

        @Override
        public org.apache.kafka.common.serialization.Deserializer deserializer() {
            return this;
        }
    }
}
