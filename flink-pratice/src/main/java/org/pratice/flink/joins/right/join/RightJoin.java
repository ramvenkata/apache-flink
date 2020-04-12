/**
 * 
 */
package org.pratice.flink.joins.right.join;

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
public class RightJoin {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataSet<Tuple2<Integer, String>> personDataSet = environment.readTextFile(parameterTool.get("personDataSet"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() {

					@Override
					public Tuple2<Integer, String> map(String value) throws Exception {
						String[] input = value.split(",");
						return new Tuple2<Integer, String>(Integer.parseInt(input[0]), input[1]);
					}
				});

		DataSet<Tuple2<Integer, String>> locationSet = environment.readTextFile(parameterTool.get("locationDataSet"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() {

					@Override
					public Tuple2<Integer, String> map(String value) throws Exception {
						String[] input = value.split(",");
						return new Tuple2<Integer, String>(Integer.parseInt(input[0]), input[1]);
					}
				});

		DataSet<Tuple3<Integer, String, String>> rightJoinResultSet = personDataSet.rightOuterJoin(locationSet).where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

					@Override
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
							Tuple2<Integer, String> location) throws Exception {

						return (person == null)
								? new Tuple3<Integer, String, String>(location.getField(0), "NULL", location.getField(1))
								: new Tuple3<Integer, String, String>(location.getField(0), person.getField(1),
										location.getField(1));
					}
				});
		
		rightJoinResultSet.writeAsCsv(parameterTool.get("output"), "\n", " ");
		
		environment.execute("Right Join");
	}

}
