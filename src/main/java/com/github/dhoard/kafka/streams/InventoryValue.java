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
import com.github.dhoard.util.ThrowableUtil;
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
            InventoryValue inventoryValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("value", inventoryValue.value);
            jsonGenerator.writeEndObject();
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
        public InventoryValue deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
            JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
            long value = jsonNode.get("value").asLong(0);

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
        public void configure(Map<String, ?> configs, boolean isKey) {
            // DO NOTHING
        }

        @SuppressWarnings("unchecked")
        @Override
        public InventoryValue deserialize(String topic, byte[] data) {
            InventoryValue result = null;

            try {
                result = objectMapper.readValue(data, InventoryValue.class);
            } catch (Throwable t) {
                ThrowableUtil.throwUnchecked(t);
            }

            return result;
        }

        @Override
        public byte[] serialize(String topic, InventoryValue data) {
            byte[] result = null;

            try {
                result = objectMapper.writeValueAsBytes(data);
            } catch (Throwable t) {
                ThrowableUtil.throwUnchecked(t);
            }

            return result;
        }

        @Override
        public void close() {
            // DO NOTHING
        }

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
