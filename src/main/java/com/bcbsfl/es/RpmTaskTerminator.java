package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RpmTaskTerminator extends TaskPurgerApp {
	public RpmTaskTerminator() {
	}

	public void terminate() throws Exception {
		while(true) {
			long start = System.currentTimeMillis();
			createDataSource();
			terminate("SSO_RPM_DELAY_TASK");
			terminate("SSO_RPM_CHECK_HOME_PLAN_ACK_TASK");
			closeDataSource();
			String message = Utils.getProperty("which.app") + ": completed processing in " + convertMilliseconds(System.currentTimeMillis() - start);
			System.out.println(message);
		}
//		new EmailSender().sendResultsEmail(recsProcessed, taskQueueStatistics, workflowQueueStatistics, System.currentTimeMillis() - start);
	}

	public void terminate(String taskType) throws Exception {
		System.out.println("*******************************************************************");
		System.out.println("Starting termination of all tasks of type " + taskType);
		System.out.println("*******************************************************************");
		long start = System.currentTimeMillis();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<String> taskIds = new ArrayList<String>(); 
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			// Turn use of the cursor on.
			st.setFetchSize(100);

			String query = "select message_id from queue_message where queue_name = '" + taskType + "'";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				taskIds.add(rs.getString("message_id"));
			}
			// Turn the cursor off.
			st.setFetchSize(0);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(con != null) {
				try {
					con.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Found " + taskIds.size() + " tasks");
		int totalRecs = taskIds.size();
		if(taskIds.size() > 0) {
			for(String taskId : taskIds) {
				recsProcessed++;
				try {
					if(this.logEachRecord) {
						System.out.println("[" + new Date().toString() + "] #" + recsProcessed + " terminating task " + taskId);
					}
					terminateTask(taskId, "FAILED_WITH_TERMINAL_ERROR");
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] " + taskType + " processed " + recsProcessed + " out of " + totalRecs;
					System.out.println(msg);
				}
			}
		}
		System.out.println("*******************************************************************");
		System.out.println("For " + taskType + ", took " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("*******************************************************************");
	}

}