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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//		 Here, you can start creating your execution plan for Flink.
//
//		 Start with getting some data from the environment, like
//		env.fromSequence(1, 10);
/*		DataStream<Long> dataStream = env.fromSequence(1, 100000);
		dataStream.print();*/
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafkaConsumer = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Double> doubleDataStream = kafkaConsumer.map(value -> {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 240.0;
            }
        });
//		doubleDataStream.print();
//        DataStream<Double> output = doubleDataStream
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .apply(new AllWindowFunction<Double, Object, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow timeWindow, Iterable<Double> iterable, Collector<Object> collector) throws Exception {
//
//                        for (Double s : iterable) {
//                            collector.collect(s);
//                        }
//                    }
//                });
        DataStream<String> output = doubleDataStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .apply(new AllWindowFunction<Double, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Double> iterable, Collector<String> collector) throws Exception {
//						double sum = 0;
//						int count = 0;
                        ArrayList<Double> list = new ArrayList<>();
                        for (Double s : iterable) {
                            list.add(s);
                        }
                        collector.collect(list.toString());
                    }
                }).process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                        RequestSpecification request = RestAssured.given();
                        request.header("Content-Type", "application/json");
                        request.body(s);
                        Response response = request.post("http://localhost:5000/");
                        collector.collect(response.getBody().toString());

                    }
                });
        output.print();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        output.sinkTo(sink);

//		output.map
//        output.print();

//		 then, transform the resulting DataStream<Long> using operations
//		 like
//		 	.filter()
//		 	.flatMap()
//		 	.window()
//		 	.process()

//		 and many more.
//		 Have a look at the programming guide:
//
//		 https://nightlies.apache.org/flink/flink-docs-stable/


        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
