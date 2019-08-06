package com.github.dhoard.kafka.streams;

import com.github.dhoard.kafka.streams.InventoryValue.JSONSerde;
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

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        File file = new File(".");

        args = new String[3];
        args[0] = file.getCanonicalPath() + "/data/skus.txt";
        args[1] = file.getCanonicalPath() + "/data/locations.txt";
        args[2] = file.getCanonicalPath() + "/data/stores.txt";

        new InventoryProducerMain().run(args);
    }

    private ArrayList<String> skuList;

    private ArrayList<String> locationList;

    private ArrayList<String> storeList;

    private KafkaProducer<InventoryKey, InventoryValue> kafkaProducer;

    public void run(String[] args) throws Exception {
        addShutdownHook();

        logger.info("loading skus (" + args[0] + ") ...");
        skuList = buildList(new FileReader(args[0]));

        logger.info("loading locations (" + args[1] + ") ...");
        locationList = buildList(new FileReader(args[1]));

        logger.info("loading stores (" + args[2] + ") ...");
        storeList = buildList(new FileReader(args[2]));

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
                String sku = randomElement(skuList);
                String location = randomElement(locationList);
                String store = randomElement(storeList);

                // Positive means increment, negative means decrement,
                long qty = randomLong(-10, 10);

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
                        Thread.sleep(randomLong(1, 1000));
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

    private static ArrayList<String> buildList(Reader reader) throws IOException {
        ArrayList<String> stringList = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(reader);

        while (true) {
            String line = bufferedReader.readLine();
            if (null == line) {
                break;
            }

            if (!line.startsWith("#")) {
                stringList.add(line.trim());
            }
        }

        return stringList;
    }

    private static String randomElement(ArrayList<String> list) {
        return list.get(random.nextInt(list.size()));
    }

    private static long randomLong(int min, int max) {
        if (max == min) {
            return min;
        }

        return + (long) (random.nextDouble() * (max - min));
    }
}

