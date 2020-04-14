/**
 * 
 */
package org.practice.flink.data.stream.split;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ramesh
 *
 */
@SuppressWarnings("deprecation")
public class SplitStreamSample {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> stream = environment.readTextFile("programs/operators/numeric_data");

		SplitStream<Integer> splitStream = stream.map(new MapFunction<String, Integer>() {

			@Override
			public Integer map(String value) throws Exception {
				return Integer.parseInt(value);
			}
		}).split(new OutputSelector<Integer>() {

			@Override
			public Iterable<String> select(Integer value) {
				List<String> OUT = new ArrayList<>();
				if (value % 2 == 0) {
					OUT.add("EVEN");
				} else {
					OUT.add("ODD");
				}
				return OUT;
			}
		});
		
		splitStream.select("EVEN").writeAsText("programs/operators/split_EVEN");
		splitStream.select("ODD").writeAsText("programs/operators/split_ODD");
		
		environment.execute("Split Stream");
	}

}
