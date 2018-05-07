package com.insight;


import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStream;



public class FlinkProcess {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start kafka producer:
        KProducer kp = new KProducer();
        kp.kafkaProducer();

        //
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ec2-34-213-54-16.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("zookeeper.connect", "ec2-34-213-54-16.us-west-2.compute.amazonaws.com:2181");
        // properties.setProperty("group.id", "flink_consumer");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("ctest",
                new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(kafkaConsumer);


//// test using MapReduce

//        DataStream<Tuple2<String, Long>> result =
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//
//                    //@Override
//                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
//                        // normalize and split the line
//                        String[] words = value.toLowerCase().split("\\W+");
//
//                        // emit the pairs
//                        for (String word : words) {
//                            //Do not accept empty word, since word is defined as primary key in C* table
//                            if (!word.isEmpty()) {
//                                out.collect(new Tuple2<String, Long>(word, 1L));
//                            }
//                        }
//                    }
//                })
//                        // group by the tuple field "0" and sum up tuple field "1"
//                        .keyBy(0)
//                        .sum(1);
// System.out.println(result.getId());

        DataStream<Tuple2<Long, String>> result =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                stream.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {

                    //@Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
                        // normalize and split the line
                        String[] words = value.toLowerCase().split("\\W+");

                        // emit the pairs
                        for (String word : words) {
                            //Do not accept empty word, since word is defined as primary key in C* table
                            if (!word.isEmpty()) {
                                out.collect(new Tuple2<Long, String>(1L, word));
                            }
                        }
                    }
                });


        //Update the results to C* sink
        CassandraSink.addSink(result)
					.setQuery("INSERT INTO playground.testTable (id, count) " +
							"values (?, ?);")
					.setHost("ec2-34-213-54-16.us-west-2.compute.amazonaws.com")
					.build();

        env.execute();

    }
}
