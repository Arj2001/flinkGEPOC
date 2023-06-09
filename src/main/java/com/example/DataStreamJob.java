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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

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
//        DataStream<String> newStream = env.fromElements("Hello World");
//        newStream.map(value -> {
//            try {
//                String pythonInterpreter = "python3";
//                // Path to the Python script you want to execute. Update it with the correct path.
//                String pythonScript = "app.py";
//
//                // Create the ProcessBuilder instance with the Python command and script arguments
//                ProcessBuilder processBuilder = new ProcessBuilder(pythonInterpreter, pythonScript);
//
//                // Start the Python process
//                Process process = processBuilder.start();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            return value;
//        });

//        env.setParallelism(3);
        /*Kafka consumer*/
        KafkaSource<String> source = KafkaSource
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(
                        OffsetsInitializer.latest()
                ).setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafkaConsumer = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<ObjectNode> jsonValue = kafkaConsumer.map(value -> {

            System.out.println("Topic recieved from main kafka time: "+ System.currentTimeMillis());
            ObjectMapper objectMapper = new ObjectMapper();
            return (ObjectNode) objectMapper.readTree(value);
        });

        DataStream<String> fhr1 = jsonValue.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                String fhr1 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr1")) {
                        fhr1 = component.get("hr").toString();
//                        collector.collect(component.get("hr").toString());
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr1 != null && utrine != null){
                    list.add(fhr1);
                    list.add(utrine);
                    System.out.println("Processed to DataStream fhr1"+ System.currentTimeMillis());
                    collector.collect(list.toString());
                }
            }
        });
        DataStream<String> fhr2 = jsonValue.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                String fhr2 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr2")) {
                        fhr2 = component.get("hr").toString();
//                        collector.collect(component.get("hr").toString());
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr2 != null && utrine != null){
                list.add(fhr2);
                list.add(utrine);
                collector.collect(list.toString());
                System.out.println("Processed to DataStream fhr2"+ System.currentTimeMillis());
                }
            }
        });
        DataStream<String> fhr3 = jsonValue.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                String fhr3 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr3")) {
                        fhr3 = component.get("hr").toString();
//                        collector.collect(component.get("hr").toString());
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr3 != null && utrine != null){
                    list.add(fhr3);
                    list.add(utrine);
                    collector.collect(list.toString());
                    System.out.println("Processed to DataStream fhr3"+ System.currentTimeMillis());
                }
            }
        });
//        fhr1.print();
//        fhr2.print();
//        fhr3.print();
        KafkaSink<String> fhr1Topic = KafkaSink
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("fhr1")
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                ).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        KafkaSink<String> fhr2Topic = KafkaSink
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("fhr2")
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                ).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        KafkaSink<String> fhr3Topic = KafkaSink
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("fhr3")
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                ).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        fhr1.sinkTo(fhr1Topic);
        fhr2.sinkTo(fhr2Topic);
        fhr3.sinkTo(fhr3Topic);

        KafkaSource<String> fhr1Source = KafkaSource
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setTopics("fhr1")
                .setGroupId("my-group")
                .setStartingOffsets(
                        OffsetsInitializer.latest()
                ).setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        KafkaSource<String> fhr2Source = KafkaSource
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setTopics("fhr2")
                .setGroupId("my-group")
                .setStartingOffsets(
                        OffsetsInitializer.latest()
                ).setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        KafkaSource<String> fhr3Source = KafkaSource
                .<String>builder()
                .setBootstrapServers("172.21.89.248:9092")
                .setTopics("fhr3")
                .setGroupId("my-group")
                .setStartingOffsets(
                        OffsetsInitializer.latest()
                ).setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> fhr1Stream = env.fromSource(fhr1Source, WatermarkStrategy.noWatermarks(), "fhr1");
        DataStream<String> fhr2Stream = env.fromSource(fhr2Source, WatermarkStrategy.noWatermarks(), "fhr2");
        DataStream<String> fhr3Stream = env.fromSource(fhr3Source, WatermarkStrategy.noWatermarks(), "fhr3");

        fhr1Stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                System.out.println("After 2nd kafka inside process method fhr1" + System.currentTimeMillis());
                RequestSpecification request = RestAssured.given();
                request.header("Content-Type", "application/json");
                request.body(s);
                try {
                    Response response = request.post("http://0.0.0.0:5000/");
                    collector.collect(response.getBody().asString());
                    System.out.println(response.getBody().asString());
                    System.out.println("fhr1 end time " + System.currentTimeMillis());
                }catch (Exception e){
                    System.out.println(e + "post is not working");
                }
            }
        });
//        fhr2Stream.process(new ProcessFunction<String, String>() {
//            @Override
//            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
//                System.out.println("After 2nd kafka inside process method fhr2" + System.currentTimeMillis());
//                RequestSpecification request = RestAssured.given();
//                request.header("Content-Type", "application/json");
//                request.body(s);
//                Response response = request.post("http://172.24.59.26:2000/");
//                collector.collect(response.getBody().asString());
//                System.out.println(response.getBody().asString());
//                System.out.println("fhr2 end time " + System.currentTimeMillis());
//            }
//        });
//        fhr3Stream.process(new ProcessFunction<String, String>() {
//            @Override
//            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
//                System.out.println("After 2nd kafka inside process method fhr3 " + System.currentTimeMillis());
//                RequestSpecification request = RestAssured.given();
//                request.header("Content-Type", "application/json");
//                request.body(s);
//                Response response = request.post("http://172.24.59.26:3000/");
//                collector.collect(response.getBody().asString());
//                System.out.println(response.getBody().asString());
//                System.out.println("fhr3 end time " + System.currentTimeMillis());
//            }
//        });

        env.execute("Flink Java API Skeleton");
    }
}
