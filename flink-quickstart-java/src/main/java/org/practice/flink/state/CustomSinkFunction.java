/**
 * 
 */
package org.practice.flink.state;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Ramesh
 *
 */
public class CustomSinkFunction implements SinkFunction<String>, CheckpointedFunction {

	/** */
	private static final long serialVersionUID = -3500013694695977149L;

	private static final int buffer_size = 10;

	/** check point or snapshot state */
	private transient ListState<String> checkPointedState;

	/** buffer */
	private List<String> bufferredElements;

	/**
	 * 
	 */
	public CustomSinkFunction() {
		this.bufferredElements = new ArrayList<>(buffer_size);
	}

	@Override
	public void invoke(String value, Context context) throws Exception {

		// write to the buffer before even writing to file system
		bufferredElements.add(value);

		if (bufferredElements.size() >= buffer_size) {
			// write to a file only when the size of the buffer is greater than 10

			bufferredElements.clear();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction#snapshotState(
	 * org.apache.flink.runtime.state.FunctionSnapshotContext)
	 */
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		
		System.out.println("Snapshot is requested @ " + LocalDateTime.now());
		checkPointedState.clear();
		
		for (String element: bufferredElements) {
			checkPointedState.add(element);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flink.streaming.api.checkpoint.CheckpointedFunction#
	 * initializeState(org.apache.flink.runtime.state.FunctionInitializationContext)
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("bufferred_elements",
				TypeInformation.of(new TypeHint<String>() {
				}));

		checkPointedState = context.getOperatorStateStore().getListState(stateDescriptor);

		if (context.isRestored()) {
			for (String element : checkPointedState.get()) {
				bufferredElements.add(element);
			}
		}

	}

}
