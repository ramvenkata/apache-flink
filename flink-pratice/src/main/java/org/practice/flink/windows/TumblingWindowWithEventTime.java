/**
 * 
 */
package org.practice.flink.windows;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Ramesh
 *
 */
public class TumblingWindowWithEventTime {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> dataStream = environment.socketTextStream("localhost", 8989);
		DataStream<Tuple2<LocalDateTime, Integer>> reducedStream = dataStream
				.map(new MapFunction<String, Tuple2<LocalDateTime, Integer>>() {

					@Override
					public Tuple2<LocalDateTime, Integer> map(String value) throws Exception {
						String[] array = value.split(",");
						Instant instant = Instant.ofEpochMilli(Long.parseLong(array[0]));
						LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
						return new Tuple2<LocalDateTime, Integer>(localDateTime, Integer.parseInt(array[1]));
					}
				}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<LocalDateTime, Integer>>() {

					@Override
					public long extractAscendingTimestamp(Tuple2<LocalDateTime, Integer> element) {
						return element.f0.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
					}
				}).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<Tuple2<LocalDateTime, Integer>>() {

					@Override
					public Tuple2<LocalDateTime, Integer> reduce(Tuple2<LocalDateTime, Integer> value1,
							Tuple2<LocalDateTime, Integer> value2) throws Exception {
						return new Tuple2<LocalDateTime, Integer>(LocalDateTime.now(), value1.f1 + value2.f1);
					}
				});
		
		reducedStream.writeAsText("programs/windows/window_event_reduced_stream", WriteMode.OVERWRITE);
		
		environment.execute("Tumbling window with Event Time");
	}

}
