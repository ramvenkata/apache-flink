/**
 * 
 */
package org.practive.flink.data.stream.fold;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * @author Ramesh
 *
 */
public class CustomFold implements
		FoldFunction<Tuple5<String, String, String, Integer, Integer>, Tuple4<String, String, Integer, Integer>> {

	@Override
	public Tuple4<String, String, Integer, Integer> fold(Tuple4<String, String, Integer, Integer> accumulator,
			Tuple5<String, String, String, Integer, Integer> value) throws Exception {

		accumulator.f0 = value.getField(0);
		accumulator.f1 = value.getField(1);
		accumulator.f2 += value.f3;
		accumulator.f3 += value.f4;

		return accumulator;
	}

}
