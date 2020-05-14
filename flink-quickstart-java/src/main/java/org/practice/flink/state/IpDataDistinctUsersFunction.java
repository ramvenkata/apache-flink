/**
 * 
 */
package org.practice.flink.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author Ramesh
 *
 */
public class IpDataDistinctUsersFunction
		extends RichFlatMapFunction<Tuple6<String, String, String, String, String, Long>, Tuple2<String, Integer>> {

	/** */
	private static final long serialVersionUID = 3263804178221212301L;
	private transient ListState<Tuple2<String, Integer>> usersState;
	private transient ValueState<Map<String, Integer>> websiteMap;

	@Override
	public void flatMap(Tuple6<String, String, String, String, String, Long> value,
			Collector<Tuple2<String, Integer>> out) throws Exception {

		Map<String, Integer> map = websiteMap.value();
		String key = value.f2;
		int count = (map.containsKey(key)) ? map.get(key) + 1 : 1;
		map.put(key, count);

		websiteMap.clear();
		websiteMap.value().putAll(map);

		usersState.clear();
		
		for (String key2 : map.keySet()) {
			usersState.add(new Tuple2<String, Integer>(key2, map.get(key2)));
		}
		
		out.collect(new Tuple2<String, Integer>(key, count));

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Map<String, Integer>> websiteMapDescriptor = new ValueStateDescriptor<>("website_map",
				TypeInformation.of(new TypeHint<Map<String, Integer>>() {
				}), new HashMap<>());

		ListStateDescriptor<Tuple2<String, Integer>> distinctUsers = new ListStateDescriptor<>("distict_users",
				TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
				}));

		websiteMap = getRuntimeContext().getState(websiteMapDescriptor);

		usersState = getRuntimeContext().getListState(distinctUsers);
	}
}
