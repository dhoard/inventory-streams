package com.github.dhoard.kafka.streams;

import com.github.dhoard.kafka.streams.InventoryValue.JSONSerde;
import com.github.dhoard.util.ListUtil;
import com.github.dhoard.util.RandomUtil;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InventoryProducerMain {

    private static final Logger logger = LoggerFactory.getLogger(InventoryProducerMain.class);

    private ArrayList<String> skuList;

    private ArrayList<String> locationList;

    private ArrayList<String> storeList;

    private KafkaProducer<InventoryKey, InventoryValue> kafkaProducer;

    public static void main(String[] args) throws Exception {
        File file = new File(".");

        args = new String[3];
        args[0] = file.getCanonicalPath() + "/data/skus.txt";
        args[1] = file.getCanonicalPath() + "/data/locations.txt";
        args[2] = file.getCanonicalPath() + "/data/stores.txt";

        new InventoryProducerMain().run(args);
    }

    public void run(String[] args) throws Exception {
        addShutdownHook();

        logger.info("loading skus (" + args[0] + ") ...");
        skuList = ListUtil.buildList(new FileReader(args[0]));

        logger.info("loading locations (" + args[1] + ") ...");
        locationList = ListUtil.buildList(new FileReader(args[1]));

        logger.info("loading stores (" + args[2] + ") ...");
        storeList = ListUtil.buildList(new FileReader(args[2]));

        KafkaProducer<InventoryKey, InventoryValue> kafkaProducer = null;

        try {
            String boostrapServers = "confluent-platform-standalone-2.address.cx:9092";

            String topic = "inventory-input";

            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                InventoryKey.JSONSerde.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JSONSerde.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());

            this.kafkaProducer = new KafkaProducer<InventoryKey, InventoryValue>(properties);

            while (true) {
                String sku = ListUtil.randomElement(skuList);
                String location = ListUtil.randomElement(locationList);
                String store = ListUtil.randomElement(storeList);

                // Positive means increment, negative means decrement,
                long qty = 1; // randomLong(-10, 10);

                // Skip values that have no change
                if (qty != 0L) {
                    InventoryKey inventoryKey = new InventoryKey();
                    inventoryKey.sku = sku;
                    inventoryKey.location = location;
                    inventoryKey.store = store;

                    InventoryValue inventoryValue = new InventoryValue();
                    inventoryValue.value = qty;

                    ProducerRecord<InventoryKey, InventoryValue> producerRecord = new ProducerRecord<InventoryKey, InventoryValue>(
                        topic, inventoryKey, inventoryValue);

                    this.kafkaProducer.send(producerRecord);

                    try {
                        Thread.sleep(RandomUtil.randomLong(1, 1000));
                    } catch (Throwable t) {
                        // DO NOTHING
                    }
                }
            }
        } finally {
            if (null != this.kafkaProducer) {
                this.kafkaProducer.close();
                this.kafkaProducer = null;
            }
        }
    }

    private void shutdown() {
        if (null != this.kafkaProducer) {
            this.kafkaProducer.flush();
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdown();
            }
        }));
    }
}

