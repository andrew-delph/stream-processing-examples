package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WordCount {

    public static void main(String[] args) throws Exception {

        String host = "localhost"; // the address of the JobManager
        int port = 8081; // the port of the JobManager
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port);
       

        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:29092")
        .setTopics("input-topic")
        .setGroupId("flink-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        DataStream<String> kafkaStream =env.fromSource(source,
        WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaStream.print();

         // Execute the job
         env.execute("My Flink Job");

    }
}
