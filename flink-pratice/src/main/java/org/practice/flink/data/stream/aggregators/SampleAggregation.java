/**
 * 
 */
package org.practice.flink.data.stream.aggregators;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.practice.flink.data.stream.reduce.CustomMapper;

/**
 * @author Ramesh
 *
 */
public class SampleAggregation {

	/**
	 * @param args
	 * @throws Exception 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		// read the data stream from the file directly
		DataStream<String> data = environment.readTextFile("programs/operators/avg");

		// map the fields into the tuple
		DataStream<Tuple5<String, String, String, Integer, Integer>> mappedData = data.map(new CustomMapper());
		
		mappedData.keyBy(0).sum(3).writeAsText("programs/operators/aggregate_sum");
		
		mappedData.keyBy(0).min(3).writeAsText("programs/operators/aggregate_min");
		
		mappedData.keyBy(0).minBy(3).writeAsText("programs/operators/aggregate_minBy");
		
		mappedData.keyBy(0).max(3).writeAsText("programs/operators/aggregate_max");
		
		mappedData.keyBy(0).maxBy(3).writeAsText("programs/operators/aggregate_maxBy");
		
		environment.execute("Aggregation Operation");
	}

}
