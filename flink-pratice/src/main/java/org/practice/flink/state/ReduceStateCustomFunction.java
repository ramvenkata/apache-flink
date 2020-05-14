/**
 * 
 */
package org.practice.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author Ramesh
 *
 */
public class ReduceStateCustomFunction extends RichFlatMapFunction<Tuple2<Long, String>, Long> {

	/** */
	private static final long serialVersionUID = -324250795171017814L;

	private transient ValueState<Long> count;
	private transient ReducingState<Long> sum;

	@Override
	public void flatMap(Tuple2<Long, String> value, Collector<Long> out) throws Exception {

		Long currentCount = count.value();
		currentCount++;

		count.update(currentCount);
		sum.add(Long.parseLong(value.f1));

		if (currentCount >= 10) {
			out.collect(sum.get());
			sum.clear();
			count.clear();
		}

	}

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>("count", Long.class);
		ReducingStateDescriptor<Long> sumDescriptor = new ReducingStateDescriptor<>("sum", new ReduceFunction<Long>() {

			/** */
			private static final long serialVersionUID = -6866577609599874483L;

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				// TODO Auto-generated method stub
				return value1 + value2;
			}
		}, Long.class);

		count = getRuntimeContext().getState(countDescriptor);
		sum = getRuntimeContext().getReducingState(sumDescriptor);

	}
}
