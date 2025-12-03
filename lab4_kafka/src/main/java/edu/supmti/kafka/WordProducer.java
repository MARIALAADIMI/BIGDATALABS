package edu.supmti.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class WordProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        System.out.println("=== Word Producer Started ===");
        System.out.println("Écris un texte (CTRL+C pour quitter):");

        while (true) {
            String line = scanner.nextLine();

            // Découper chaque mot وإرساله
            String[] words = line.split("\\W+");

            for (String word : words) {
                if (!word.isEmpty()) {
                    producer.send(new ProducerRecord<>("WordCount-Topic", word));
                    System.out.println("Envoyé : " + word);
                }
            }
        }
    }
}
