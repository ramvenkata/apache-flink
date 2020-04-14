/**
 * 
 */
package org.practice.flink.word.count;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ramesh
 *
 */
public class WordCountStream {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterTool.fromArgs(args);

		environment.getConfig().setGlobalJobParameters(params);

		DataStream<String> text = environment.socketTextStream("192.168.0.21", 9231);

		DataStream<Tuple2<String, Integer>> filtered = text.filter(new FilterFunction<String>() {
			public boolean filter(String value) {
				return value.startsWith("N");
			}
		}).map(new Tokenizer()).keyBy(0).sum(1);

		filtered.print();
		
		environment.execute("Streaming WordCount");
	}

	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> map(String value) {
			return new Tuple2(value, Integer.valueOf(1));
		}
	}

}
