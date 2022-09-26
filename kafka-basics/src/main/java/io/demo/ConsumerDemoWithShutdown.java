package io.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String topic = "demo";
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-app";

        // Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference of the current thread
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //Subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singletonList(topic));

            //poll for new data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record: records) {
                    log.info("key: " + record.key() + " value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("wake up exception");
        } catch (Exception e) {
            log.info("Unexpected exception");
            e.printStackTrace();
        } finally {
            consumer.close();
            log.info("The consumer is closed.");
        }

    }
}
