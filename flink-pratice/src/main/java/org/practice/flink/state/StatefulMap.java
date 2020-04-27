/**
 * 
 */
package org.practice.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author Ramesh
 *
 */
public class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {

	private ValueState<Long> sum;
	private ValueState<Long> count;

	@Override
	public void flatMap(Tuple2<Long, String> value, Collector<Long> out) throws Exception {

		Long current_sum = sum.value();
		Long current_count = count.value();

		current_sum += Long.parseLong(value.f1);
		current_count++;

		sum.update(current_sum);
		count.update(current_count);

		if (current_count >= 10) {
			out.collect(sum.value());
			sum.clear();
			count.clear();
		}
	}

	public void open(Configuration conf) {
		ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("sum",
				TypeInformation.of(new TypeHint<Long>() {
				}), 0L);
		sum = getRuntimeContext().getState(descriptor);

		ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count",
				TypeInformation.of(new TypeHint<Long>() {
				}), 0L);
		count = getRuntimeContext().getState(descriptor2);
	}

}
