/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.ArrayList;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    long time;

    public void job() throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(3);
        /*Kafka consumer*/
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("localhost:9092").setTopics("input-topic").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStream<String> kafkaConsumer = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Double> doubleDataStream = kafkaConsumer.map(value -> {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 240.0;
            }
        });

        DataStream<String> output = doubleDataStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new AllWindowFunction<Double, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Double> iterable, Collector<String> collector) throws Exception {
                long time = System.currentTimeMillis();
                ArrayList<Double> list = new ArrayList<>();
                for (Double s : iterable) {
                    list.add(s);
                }
                RequestSpecification request = RestAssured.given();
                request.header("Content-Type", "application/json");
                request.body(list.toString());
//                long time = System.currentTimeMillis();
                System.out.println("Time is taken" + (System.currentTimeMillis() - time) + "ms \n \n \n");
                Response response = request.post("http://localhost:5000/");
                collector.collect(response.getBody().asString());
                collector.collect(list.toString());

            }
        });




        output.print();
        /*kafkaProducer*/
        KafkaSink<String> sink = KafkaSink.<String>builder().setBootstrapServers("localhost:9092").setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("output-topic").setValueSerializationSchema(new SimpleStringSchema()).build()).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
        output.sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
