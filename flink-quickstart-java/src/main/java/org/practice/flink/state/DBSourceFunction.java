/**
 * 
 */
package org.practice.flink.state;

import java.sql.Connection;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author Ramesh
 *
 */
public class DBSourceFunction extends RichParallelSourceFunction<String> implements CheckpointedFunction {

	/** */
	private static final long serialVersionUID = -2394425330934995814L;

	/** check point or snapshot state */
	private transient ListState<String> checkPointedState;

	/** restored state */
	private List<String> restoredState;

	private String db_url = null;
	private String db_user = null;
	private String db_password = null;

	@Override
	public void open(Configuration configuration) throws Exception {
	//	this.connection = DBHelper.getDBConnection(configuration);
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {


	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flink.streaming.api.checkpoint.CheckpointedFunction#
	 * initializeState(org.apache.flink.runtime.state.FunctionInitializationContext)
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("my_custom_check_point_state",
				TypeInformation.of(new TypeHint<String>() {
				}));

		checkPointedState = context.getOperatorStateStore().getListState(stateDescriptor);

		if (context.isRestored()) {
			for (String element : checkPointedState.get()) {
				restoredState.add(element);
			}
		}

	}

}
