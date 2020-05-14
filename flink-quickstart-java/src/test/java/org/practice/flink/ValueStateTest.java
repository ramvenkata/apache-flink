/**
 * 
 */
package org.practice.flink;

import static org.junit.Assert.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.practice.flink.state.StatefulMap;

/**
 * @author Ramesh
 *
 */
public class ValueStateTest {

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(2).setNumberTaskManagers(1)
					.build());

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test_value_state() throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		environment.setParallelism(1);

		DataStream<String> dataStream = environment.fromElements("1,2", "1,3", "1,4", "2,3", "2,4", "2,5");
		
		DataStream<Long> resultStream = dataStream.map(new MapFunction<String, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> map(String value) throws Exception {
				String[] array = value.split(",");
				return new Tuple2<Long, String>(Long.parseLong(array[0]), array[1]);
			}
		}).keyBy(0).flatMap(new StatefulMap());
		
		resultStream.print();
		System.out.println("resultStream => " + resultStream);
		//writeAsText("programs/state/state_result_stream", WriteMode.OVERWRITE);
		
		environment.execute("State Test");
	}

}
