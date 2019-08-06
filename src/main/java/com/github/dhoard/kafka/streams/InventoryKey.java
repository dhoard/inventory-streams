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
            InventoryKey value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {

            jgen.writeStartObject();
            jgen.writeStringField("sku", value.sku);
            jgen.writeStringField("location", value.location);
            jgen.writeStringField("store", value.store);
            jgen.writeEndObject();
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
        public InventoryKey deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
            JsonNode node = jp.getCodec().readTree(jp);
            String store = node.get("store").asText();
            String location = node.get("location").asText();
            String sku = node.get("sku").asText();

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
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public InventoryKey deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.readValue(data, InventoryKey.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final InventoryKey data) {
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
        public Serializer<InventoryKey> serializer() {
            return this;
        }

        @Override
        public Deserializer<InventoryKey> deserializer() {
            return this;
        }
    }
}
