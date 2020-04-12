/**
 * 
 */
package org.practice.flink.joins.inner.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author Ramesh
 *
 */
public class InnerJoin {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		// get the execution environment
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		// read the parameters for args
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		// read person data set
		DataSet<Tuple2<Integer, String>> personDataSet = environment.readTextFile(parameterTool.get("input1"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 4896835609936237250L;

					@Override
					public Tuple2<Integer, String> map(String readLine) throws Exception {
						String[] input = readLine.split(",");
						return new Tuple2<Integer, String>(Integer.parseInt(input[0]), input[1]);
					}
				});

		// read location data set
		DataSet<Tuple2<Integer, String>> locationDataSet = environment.readTextFile(parameterTool.get("input2"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 3579442725484377297L;

					@Override
					public Tuple2<Integer, String> map(String readLine) throws Exception {
						String[] input = readLine.split(",");
						return new Tuple2<Integer, String>(Integer.parseInt(input[0]), input[1]);
					}

				});

		// now join both data sets using the keys/ value0 in both data sets
		DataSet<Tuple3<Integer, String, String>> joinedDataSet = personDataSet.join(locationDataSet).where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

					@Override
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
							Tuple2<Integer, String> location) throws Exception {
						return new Tuple3<Integer, String, String>(person.getField(0), person.getField(1),
								location.getField(1));
					}

				});
		
		joinedDataSet.writeAsCsv(parameterTool.get("output"), "\n", " ");
		
		environment.execute("Inner Join ");
	}

}
