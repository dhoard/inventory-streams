package com.github.dhoard.kafka.streams;

import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InventoryConsumerMain {

    private static final Logger logger = LoggerFactory.getLogger(InventoryConsumerMain.class);

    public static void main(String[] args) throws Exception {

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    new InventoryConsumerMain().run(new String[] { "inventory-output" });
                } catch (Throwable t) {
                    logger.error("Exception", t);
                }
            }
        });

        thread.start();

        synchronized (Thread.currentThread()) {
            Thread.currentThread().wait();
        }
    }

    private KafkaConsumer<InventoryKey, InventoryValue> kafkaConsumer = null;

    private void run(String[] args) throws Exception {
        addShutdownHook();

        String bootstrapServers = "confluent-platform-standalone-2.address.cx:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.setProperty(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, InventoryKey.JSONSerde.class.getName());

        properties.setProperty(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, InventoryValue.JSONSerde.class.getName());

        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            MonitoringConsumerInterceptor.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args[0]);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            this.kafkaConsumer = new KafkaConsumer<InventoryKey, InventoryValue>(properties);
            this.kafkaConsumer.subscribe(Collections.singleton(args[0]));

            while (true) {
                ConsumerRecords<InventoryKey, InventoryValue> consumerRecords = kafkaConsumer
                    .poll(Duration.ofMillis(100));

                for (ConsumerRecord<InventoryKey, InventoryValue> consumerRecord : consumerRecords) {
                    String store = consumerRecord.key().store;
                    String location = consumerRecord.key().location;
                    String sku = consumerRecord.key().sku;
                    long value = consumerRecord.value().value;

                    if (store.equals("Lexington") && location.equals("front of house")) {
                        if ("66561883561927".equals(sku)) {
                            logger.info("---------------------------");
                            logger.info(
                                "store    -> " + store);
                            logger.info(
                                "location -> " + location);
                            logger.info(
                                "sku      -> " + sku);
                            logger.info(
                                "qty      -> " + value);
                        }
                    }
                }
            }
        } finally {
            if (null != this.kafkaConsumer) {
                try {
                    this.kafkaConsumer.close();
                } catch (Throwable t) {
                    logger.error("Exception closing kafkaConsumer");
                }
            }
        }
    }

    private void shutdown() {
        this.kafkaConsumer.wakeup();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdown();
            }
        }));
    }

    private String padLeft(String value, int length, String s) {
        String result = value;

        while (result.length() < length) {
            result = s + result;
        }

        return result;
    }
}
