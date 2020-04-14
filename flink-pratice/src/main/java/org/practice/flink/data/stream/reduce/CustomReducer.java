/**
 * 
 */
package org.practice.flink.data.stream.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * @author Ramesh
 *
 */
public class CustomReducer implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7297288621592509899L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flink.api.common.functions.ReduceFunction#reduce(java.lang.Object,
	 * java.lang.Object)
	 */
	@Override
	public Tuple5<String, String, String, Integer, Integer> reduce(
			Tuple5<String, String, String, Integer, Integer> current,
			Tuple5<String, String, String, Integer, Integer> previous_result) throws Exception {

		return new Tuple5<String, String, String, Integer, Integer>(current.getField(0), current.getField(1),
				current.getField(2), current.f3 + previous_result.f3, current.f4 + previous_result.f4);
	}

}
