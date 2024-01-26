
package org.myorg.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
