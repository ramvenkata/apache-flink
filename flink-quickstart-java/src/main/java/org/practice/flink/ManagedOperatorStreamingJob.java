/**
 * 
 */
package org.practice.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.practice.flink.state.CustomSinkFunction;

/**
 * @author Ramesh
 *
 */
public class ManagedOperatorStreamingJob {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		environment.enableCheckpointing(100);
		
		environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		
		environment.getCheckpointConfig().setCheckpointTimeout(1000);
		
		environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		
		environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		
		environment.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
		
		environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10));
		
		environment.setStateBackend(new FsStateBackend("file:///flink/temp-backend/", false));

		environment.addSource(new RichParallelSourceFunction<String>() {

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void cancel() {
				// TODO Auto-generated method stub
				
			}
		});
		DataStream<String> dataStream = environment.socketTextStream("localhost", 8989).uid("data-from-socketstream");

		dataStream.addSink(new CustomSinkFunction());
		
		environment.execute("Managed Operator State Sample Flink Job");
	}

}
