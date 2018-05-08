package com.insight;


import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStream;


public class FlinkProcess {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start kafka producer !!!!!!!!! using python producer
        //KProducer kp = new KProducer();
        //kp.kafkaProducer();

        // create new property of Flink
        // set zookeeper and flink url
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ec2-34-213-54-16.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("zookeeper.connect", "ec2-34-213-54-16.us-west-2.compute.amazonaws.com:2181");
        //properties.setProperty("group.id", "flink_consumer");


        // read 'topic' from Kafka producer
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("ctest",
                new SimpleStringSchema(), properties);

        // convert kafka stream to data stream
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer);

        // inputs are 'revision', 'article_id', 'rev_id', 'article title', 'timestamp',  'username', 'userid'
        DataStream<Tuple2<String, Long>> windowedoutput = rawInputStream
                .map(line -> line.split(" "))
                .flatMap(new FlatMapFunction<String[], Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String[] s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        collector.collect(new Tuple2<>(s[5], 1L));
                    }
                })
                .keyBy(0)
                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.of(5, TimeUnit.SECONDS))
                .sum(1);


        // count the total number of processed line in window of 5 sec
        DataStream<Tuple2<Long, Long>> winOutput = rawInputStream
                .flatMap(new FlatMapFunction<String, Tuple2<Long, Long>>() {

                    // Create an accumulator
                    private long linesNum = 0;


                    @Override
                    public void flatMap(String lines, Collector<Tuple2<Long, Long>> out) throws Exception {
                        out.collect(new Tuple2<Long, Long>(linesNum++, 1L));

                    }
                })
                .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.of(10, TimeUnit.MILLISECONDS))
                .sum(1);



//        // read in raw input, then parse it to save only 3 elements: article title, timestamp, username
//        DataStream<Tuple3< String, String, String>> output = rawInputStream
//                .map(line -> line.split(" "))
//                .flatMap(new FlatMapFunction<String[], Tuple3<String, String, String>>() {
//                    @Override
//                    public void flatMap(String[] s, Collector<Tuple3< String, String, String>> collector) throws Exception {
//                        collector.collect(new Tuple3<String, String, String>(s[3], s[4], s[5]));
//                    }
//                });


//        // Tuple2 save two elements pair
//        DataStream<Tuple2<Long, String>> result =
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                rawInputStream.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
//                    //@Override
//                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
//                        // normalize and split the line
//                        String[] words = value.toLowerCase().split("\\W+");
//
//                        // emit the pairs
//                        for (String word : words) {
//                            //Do not accept empty word, since word is defined as primary key in C* table
//                            if (!word.isEmpty()) {
//                                out.collect(new Tuple2<Long, String>(1L, word));
//                            }
//                        }
//                    }
//                });


//        //Update the results to C* sink
//        CassandraSink.addSink(result)
//                .setQuery("INSERT INTO playground.testTable (id, count) " +
//                        "values (?, ?);")
//                .setHost("ec2-34-213-54-16.us-west-2.compute.amazonaws.com")
//                .build();

//        // update output to cassandra:
//                CassandraSink.addSink(output)
//                .setQuery("INSERT INTO playground.testTable2 ( arttitle, time, username) " +
//                        "values (?, ?, ?);")
//                .setHost("ec2-34-213-54-16.us-west-2.compute.amazonaws.com")
//                .build();


        // save window aggregation results:
        CassandraSink.addSink(winOutput)
                .setQuery("INSERT INTO playground.testTable3 ( id, count) " +
                        "values (?, ?);")
                .setHost("ec2-34-213-54-16.us-west-2.compute.amazonaws.com")
                .build();


        env.execute();

    }



}
