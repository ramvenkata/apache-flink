/**
 * 
 */
package org.practice.flink.data.stream.iterate;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ramesh
 *
 */
public class IterateStream {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Long, Integer>> dataStream = environment.generateSequence(0, 9)
				.map(new MapFunction<Long, Tuple2<Long, Integer>>() {

					@Override
					public Tuple2<Long, Integer> map(Long value) throws Exception {
						return new Tuple2<Long, Integer>(value, 0); // 0 iterations
					}
				});

		// prepare for iteration
		IterativeStream<Tuple2<Long, Integer>> iterativeStream = dataStream.iterate(5000);

		// define iteration
		DataStream<Tuple2<Long, Integer>> iteration = iterativeStream
				.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {

					@Override
					public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
						return (value.f0 == 10) ? value : new Tuple2<Long, Integer>(value.f0 + 1, value.f1 + 1);
					}
				});
		
		// determine feedback stream - part of stream used for next iteration
		DataStream<Tuple2<Long, Integer>> nextIteration = iteration.filter(new FilterFunction<Tuple2<Long,Integer>>() {
			
			@Override
			public boolean filter(Tuple2<Long, Integer> value) throws Exception {
				return (value.f0 != 10);
			}
		});
		
		// feedback to original iterativeStream
		iterativeStream.closeWith(nextIteration);
		
		// determine fitered stream
		DataStream<Tuple2<Long, Integer>> resultStream = iteration.filter(new FilterFunction<Tuple2<Long,Integer>>() {

			@Override
			public boolean filter(Tuple2<Long, Integer> value) throws Exception {
				return (value.f0 == 10);
			}
		});
		
		resultStream.writeAsText("programs/operators/iteration_result", WriteMode.OVERWRITE);
		
		environment.execute("Iterative Stream");
	}

}
