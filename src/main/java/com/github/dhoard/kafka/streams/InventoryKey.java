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

public class InventoryKey {

    String store;

    String location;

    String sku;

    public InventoryKey() {
        // DO NOTHING
    }

    public InventoryKey(String store, String location, String sku) {
        this.store = store;
        this.location = location;
        this.sku = sku;
    }

    public static class JSONSerializer extends StdSerializer<InventoryKey> {

        public JSONSerializer() {
            this(null);
        }

        public JSONSerializer(Class<InventoryKey> t) {
            super(t);
        }

        @Override
        public void serialize(
            InventoryKey inventoryKey, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("sku", inventoryKey.sku);
            jsonGenerator.writeStringField("location", inventoryKey.location);
            jsonGenerator.writeStringField("store", inventoryKey.store);
            jsonGenerator.writeEndObject();
        }
    }

    public static class JSONDeserializer extends StdDeserializer<InventoryKey> {

        public JSONDeserializer() {
            this(null);
        }

        public JSONDeserializer(Class<InventoryKey> t) {
            super(t);
        }

        @Override
        public InventoryKey deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
            JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
            String store = jsonNode.get("store").asText();
            String location = jsonNode.get("location").asText();
            String sku = jsonNode.get("sku").asText();

            return new InventoryKey(store, location, sku);
        }
    }

    public static class JSONSerde implements Serializer<InventoryKey>,
        Deserializer<InventoryKey>, Serde<InventoryKey> {

        private static ObjectMapper objectMapper = new ObjectMapper();

        static {
            SimpleModule simpleModule = new SimpleModule();
            simpleModule.addDeserializer(InventoryKey.class, new JSONDeserializer());
            simpleModule.addSerializer(InventoryKey.class, new JSONSerializer());

            objectMapper.registerModule(simpleModule);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // DO NOTHING
        }

        @SuppressWarnings("unchecked")
        @Override
        public InventoryKey deserialize(String topic, byte[] data) {
            InventoryKey result = null;

            try {
                result = objectMapper.readValue(data, InventoryKey.class);
            } catch (Throwable t) {
                ThrowableUtil.throwUnchecked(t);
            }

            return result;
        }

        @Override
        public byte[] serialize(String topic, InventoryKey data) {
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
        public Serializer<InventoryKey> serializer() {
            return this;
        }

        @Override
        public Deserializer<InventoryKey> deserializer() {
            return this;
        }
    }
}
