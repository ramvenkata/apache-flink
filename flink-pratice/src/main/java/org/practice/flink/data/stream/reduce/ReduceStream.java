/**
 * 
 */
package org.practice.flink.data.stream.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ramesh
 *
 */
public class ReduceStream {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);
		
		// read the data stream from the file directly
		DataStream<String> data = environment.readTextFile("programs/operators/avg");
		
		// map the fields into the tuple
		DataStream<Tuple5<String,String,String, Integer, Integer>> mappedData = data.map(new CustomMapper());

		// apply reduce 
		DataStream<Tuple5<String,String,String, Integer, Integer>> reducedData = mappedData.keyBy(0).reduce(new CustomReducer());
		
		// calculate necessary result
		@SuppressWarnings("serial")
		DataStream<Tuple2<String, Double>> result = reducedData.map(new MapFunction<Tuple5<String,String,String,Integer,Integer>, Tuple2<String, Double>>() {

			@Override
			public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> value) throws Exception {
				return new Tuple2<String, Double>(value.getField(0), new Double(value.f3 * 1.0/value.f4));
			}
		});
		
		result.print();
		
		environment.execute("Custom Reducer");
	}

}
