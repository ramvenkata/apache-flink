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

package org.practice.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.practice.flink.state.CustomSinkFunction;
import org.practice.flink.state.IpDataDistinctUsersFunction;
import org.practice.flink.state.ListStateCustomFunction;
import org.practice.flink.state.ReduceStateCustomFunction;
import org.practice.flink.state.StatefulMap;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 * <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);

		// environment.setParallelism(2);
		environment.enableCheckpointing(100);
		
		environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		
		environment.getCheckpointConfig().setCheckpointTimeout(1000);
		
		environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		
		environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		
		environment.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
		
		environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10));
		
		environment.setStateBackend(new FsStateBackend("file:///flink/temp-backend/", false));

		/*DataStream<String> dataStream = environment.socketTextStream("localhost", 8989).uid("data-from-socketstream");
		DataStream<Long> valueStateResult = dataStream.map(new MapFunction<String, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> map(String value) throws Exception {
				String[] array = value.split(",");
				return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
			}
		}).uid("mapped-data-stream").keyBy(0).flatMap(new StatefulMap()).uid("flattened-data-stream");

		valueStateResult.writeAsText("programs/state/state_result_stream", WriteMode.OVERWRITE);

		DataStream<Tuple2<String, Long>> listStateResult = dataStream
				.map(new MapFunction<String, Tuple2<Long, String>>() {

					@Override
					public Tuple2<Long, String> map(String value) throws Exception {
						String[] array = value.split(",");
						return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
					}
				}).keyBy(0).flatMap(new ListStateCustomFunction());

		listStateResult.print();

		DataStream<Long> reducedStateResult = dataStream.map(new MapFunction<String, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> map(String value) throws Exception {
				String[] array = value.split(",");
				return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
			}
		}).keyBy(0).flatMap(new ReduceStateCustomFunction());

		reducedStateResult.writeAsText("programs/state/reduce_state_result_stream", WriteMode.OVERWRITE);*/

		// ## user_id,network_name,user_IP,user_country,website, Time spent before next
		// click
		//DataStream<String> ipRawDataStream = environment.readTextFile(params.get("path_to_ip_data_file"));

		/*DataStream<Tuple6<String, String, String, String, String, Long>> ipDataStream = ipRawDataStream
				.map(new MapFunction<String, Tuple6<String, String, String, String, String, Long>>() {

					@Override
					public Tuple6<String, String, String, String, String, Long> map(String value) throws Exception {
						String[] input = value.split(",");
						return new Tuple6<String, String, String, String, String, Long>(input[0], input[1], input[2],
								input[3], input[4], Long.parseLong(input[5]));
					}
				});

		DataStream<Tuple2<String, Integer>> numberOfClicksPerSite = ipDataStream
				.map(new MapFunction<Tuple6<String, String, String, String, String, Long>, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> map(Tuple6<String, String, String, String, String, Long> value)
							throws Exception {
						return new Tuple2<String, Integer>(value.f4, 1);
					}
				}).keyBy(0).sum(1);
				
				//window(TumblingProcessingTimeWindows.of(Time.milliseconds(5))).sum(1);
		
		numberOfClicksPerSite.print();
		
		DataStream<Tuple2<String, Integer>> websiteWithMaxClicks = numberOfClicksPerSite.keyBy(0).max(1);
		
		websiteWithMaxClicks.print();
		
		DataStream<Tuple2<String, Integer>> websiteWithMinClicks = numberOfClicksPerSite.keyBy(0).min(1);
		
		websiteWithMinClicks.print();
		
		ipDataStream.keyBy(0).flatMap(new IpDataDistinctUsersFunction()).print();*/
		
		DataStream<String> dataStream = environment.socketTextStream("localhost", 8989).uid("data-from-socketstream");
		
		dataStream.addSink(new CustomSinkFunction());
		
		environment.execute("State Test");

	}
}
