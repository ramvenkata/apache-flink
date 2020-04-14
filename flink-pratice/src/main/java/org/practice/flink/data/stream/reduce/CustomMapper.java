/**
 * 
 */
package org.practice.flink.data.stream.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * @author Ramesh
 *
 */
public class CustomMapper implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4454824267983696080L;

	@Override
	public Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception {
		String[] array = value.split(",");
		return new Tuple5<String, String, String, Integer, Integer>(array[1], array[2], array[3],
				Integer.parseInt(array[4]), 1);
	}

}
