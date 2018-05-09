package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.Properties;

public class KProducer {
    public void kafkaProducer() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers","ec2-50-112-36-122.us-west-2.compute.amazonaws.com:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Producer test with reading from txt file:
        //FileInputStream fis = new FileInputStream("input.txt");
        FileInputStream fis = new FileInputStream("input.txt");

        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        int lineCount = 0;
        String line = null;
        while ((line = br.readLine()) != null) {
            lineCount++;

            producer.send(new ProducerRecord<String, String>("ctest", Integer.toString(lineCount), line));
            System.out.println(new ProducerRecord<String, String>("ctest", Integer.toString(lineCount), line));
        }

//        Producer test with input of 1 - 10:
//        for(int i = 0; i< 10; i++) {
//            producer.send(new ProducerRecord<String, String>("ctest", Integer.toString(i), Integer.toString(i)));
//
//            System.out.println(new ProducerRecord<String, String>("ctest", Integer.toString(i), Integer.toString(i)));
//        }
        producer.flush();
        producer.close();
    }
}
