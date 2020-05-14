/**
 * 
 */
package org.practice.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Ramesh
 *
 */
public class IPDataStreamingJob {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// define source
		DataStream<String> rawDataStream = environment.readTextFile("");

		// map it accordingly
		DataStream<IPDataModel> ipDataStream = rawDataStream.map(new MapFunction<String, IPDataModel>() {

			/** */
			private static final long serialVersionUID = -5128027075801740094L;

			@Override
			public IPDataModel map(String value) throws Exception {
				String[] data = value.split(",");
				return new IPDataModel(data[0], data[1], data[2], data[3], data[4], Long.parseLong(data[5]));
			}
		});

		
		ipDataStream.keyBy(new KeySelector<IPDataModel, String>() {

			@Override
			public String getKey(IPDataModel value) throws Exception {
				// TODO Auto-generated method stub
				return value.getWebsite();
			}
		}).flatMap(new FlatMapFunction<IPDataModel, Long>() {

			@Override
			public void flatMap(IPDataModel value, Collector<Long> out) throws Exception {
				// TODO Auto-generated method stub
				
			}
		});
		
	}

}
