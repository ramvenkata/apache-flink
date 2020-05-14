/**
 * 
 */
package org.practice.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ramesh
 *
 */
public class StateDemo {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataStream = environment.socketTextStream("localhost", 8989);
		DataStream<Long> resultStream = dataStream.map(new MapFunction<String, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> map(String value) throws Exception {
				String[] array = value.split(",");
				return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
			}
		}).keyBy(0).flatMap(new StatefulMap());
		
		resultStream.print();
		//writeAsText("programs/state/state_result_stream", WriteMode.OVERWRITE);
		
		environment.execute();

	}

}
