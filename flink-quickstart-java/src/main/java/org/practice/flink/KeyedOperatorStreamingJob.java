/**
 * 
 */
package org.practice.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.practice.flink.state.ListStateCustomFunction;
import org.practice.flink.state.ReduceStateCustomFunction;
import org.practice.flink.state.StatefulMap;

/**
 * @author Ramesh
 *
 */
public class KeyedOperatorStreamingJob {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataStream = environment.socketTextStream("localhost", 8989).uid("data-from-socketstream");
		DataStream<Long> valueStateResult = dataStream.map(new MapFunction<String, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> map(String value) throws Exception {
				String[] array = value.split(",");
				return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
			}
		}).uid("mapped-data-stream").keyBy(0).flatMap(new StatefulMap()).uid("flattened-data-stream");

		// valueStateResult.writeAsText("programs/state/state_result_stream",
		// WriteMode.OVERWRITE);

		valueStateResult.print();

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

		// reducedStateResult.writeAsText("programs/state/reduce_state_result_stream",
		// WriteMode.OVERWRITE);

		reducedStateResult.print();

		environment.execute("Keyed State Sample Flink Job");
	}

}
