/**
 * 
 */
package org.practice.flink.state;

import java.util.StringJoiner;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
public class ListStateCustomFunction extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<String, Long>> {

	/** */
	private static final long serialVersionUID = -3619698018085777081L;

	private transient ValueState<Long> count;
	private transient ListState<Long> numbers;

	@Override
	public void flatMap(Tuple2<Long, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
		// TODO Auto-generated method stub

		Long currentCount = count.value();
		currentCount++;

		count.update(currentCount);
		numbers.add(Long.parseLong(value.f1));

		if (currentCount >= 10) {
			StringJoiner sj = new StringJoiner(",", "[", "]");
			Long sum = 0L;
			for (Long number : numbers.get()) {
				sum = sum + number;
				sj.add("" + number);
			}

			out.collect(new Tuple2<String, Long>(sj.toString(), sum));
			
			count.clear();
			numbers.clear();
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>("count",
				TypeInformation.of(Long.class));
		ListStateDescriptor<Long> listDescriptor = new ListStateDescriptor<>("numbers",
				TypeInformation.of(new TypeHint<Long>() {
				}));

		count = getRuntimeContext().getState(countDescriptor);
		numbers = getRuntimeContext().getListState(listDescriptor);

	}

}
