package edu.supmti.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.*;

public class WordCountConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        props.put("group.id", "wordcount-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("WordCount-Topic"));

        Map<String, Integer> wordCounts = new HashMap<>();

        System.out.println("=== Word Count Consumer Started ===");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                String word = record.value();

                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);

                System.out.println("Mot: " + word + "  |  fréquence: " + wordCounts.get(word));
            }
        }
    }
}
