package com.github.dhoard.kafka.streams;

import com.github.dhoard.util.TimestampUtil;
import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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

    private BufferedWriter bufferedWriter;

    private KafkaConsumer<InventoryKey, InventoryValue> kafkaConsumer;

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
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        BufferedWriter bufferedWriter = null;

        try {
            new File(new File(".").getCanonicalPath() + "/output").mkdir();

            this.bufferedWriter =
                new BufferedWriter(
                    new FileWriter(
                        new File(".").getCanonicalFile() + "/output/output.txt"));

            this.kafkaConsumer = new KafkaConsumer<InventoryKey, InventoryValue>(properties);
            this.kafkaConsumer.subscribe(Collections.singleton(args[0]));

            StringBuilder stringBuilder = new StringBuilder();
            String line = null;

            while (true) {
                ConsumerRecords<InventoryKey, InventoryValue> consumerRecords = kafkaConsumer
                    .poll(Duration.ofMillis(100));

                for (ConsumerRecord<InventoryKey, InventoryValue> consumerRecord : consumerRecords) {
                    String store = consumerRecord.key().store;
                    String location = consumerRecord.key().location;
                    String sku = consumerRecord.key().sku;
                    long value = consumerRecord.value().value;

                    stringBuilder.setLength(0);

                    stringBuilder.append(TimestampUtil.getISOTimestamp());
                    stringBuilder.append(", ");
                    stringBuilder.append(store);
                    stringBuilder.append(", ");
                    stringBuilder.append(location);
                    stringBuilder.append(", ");
                    stringBuilder.append(sku);
                    stringBuilder.append(", ");
                    stringBuilder.append(value);
                    stringBuilder.append(System.lineSeparator());

                    line = stringBuilder.toString();

                    System.out.print(line);
                    this.bufferedWriter.write(line);
                }

                this.bufferedWriter.flush();
            }
        } finally {
            if (null != this.bufferedWriter) {
                try {
                    this.bufferedWriter.close();
                } catch (Throwable t) {
                    logger.error("Exception closing bufferedWriter");
                }
            }

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
