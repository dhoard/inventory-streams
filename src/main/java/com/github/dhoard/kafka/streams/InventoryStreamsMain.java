package com.github.dhoard.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dhoard.kafka.streams.InventoryValue.JSONSerde;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InventoryStreamsMain {

    private final static Logger logger = LoggerFactory.getLogger(InventoryStreamsMain.class);

    private StreamsBuilder streamsBuilder;

    private KStream<InventoryKey, InventoryValue> inventoryKeyInventoryValueKStream;

    public static void main(String[] args) throws Throwable {
        new InventoryStreamsMain().run(args);
    }

    public void run(String[] args) throws Throwable {
        String bootstrapServers = "confluent-platform-standalone-2.address.cx:9092";
        String applicationId = "inventory-streams-project-1";
        Class defaultKeySerde = InventoryKey.JSONSerde.class;
        Class defaultValueSerde = JSONSerde.class;
        String autoOffsetResetConfig = "earliest";

        logger.info("bootstrapServers      = [" + bootstrapServers + "]");
        logger.info("autoOffsetResetConfig = [" + autoOffsetResetConfig + "]");
        logger.info("applicationId         = [" + applicationId + "]");
        logger.info("defaultKeySerde       = [" + defaultKeySerde + "]");
        logger.info("defaultValueSerde     = [" + defaultValueSerde + "]");

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerde);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        properties.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        String inputTopic = "inventory-input";
        String outputTopic = "inventory-output";

        logger.info("inputTopic  = [" + inputTopic + "]");
        logger.info("outputTopic = [" + outputTopic + "]");

        this.streamsBuilder = new StreamsBuilder();

        this.inventoryKeyInventoryValueKStream =
            streamsBuilder.stream(
                inputTopic,
                Consumed.with(new InventoryKey.JSONSerde(), new JSONSerde()));

        this.inventoryKeyInventoryValueKStream
            .groupByKey()
            .aggregate(
                InventoryValue::new, (key, value, aggr) -> {
                aggr.value += value.value;
                return aggr;
            }).toStream().to(outputTopic, Produced.with(new InventoryKey.JSONSerde(), new JSONSerde()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

        logger.info(kafkaStreams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
