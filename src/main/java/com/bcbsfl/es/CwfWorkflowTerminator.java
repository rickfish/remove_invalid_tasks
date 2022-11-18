package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CwfWorkflowTerminator extends TaskPurgerApp {
	public CwfWorkflowTerminator() {
	}

	public void terminate() throws Exception {
		long start = System.currentTimeMillis();
		createDataSource();
		terminate("cwf_entry_exception_workflow_1.0");
		terminate("claims_workflow_task_creation_workflow_1.0");
		closeDataSource();
		String message = Utils.getProperty("which.app") + ": completed processing in " + convertMilliseconds(System.currentTimeMillis() - start);
		System.out.println(message);
//		new EmailSender().sendResultsEmail(recsProcessed, taskQueueStatistics, workflowQueueStatistics, System.currentTimeMillis() - start);
	}

	public void terminate(String workflowType) throws Exception {
		System.out.println("*******************************************************************");
		System.out.println("Starting termination of all workflows of type " + workflowType);
		System.out.println("*******************************************************************");
		long start = System.currentTimeMillis();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<String> workflowIds = new ArrayList<String>(); 
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			// Turn use of the cursor on.
			st.setFetchSize(100);

			String query = "select workflow_id from queue_message qm inner join workflow_def_to_workflow wd on wd.workflow_id = qm.message_id where queue_name = '_deciderQueue' and workflow_def = '" + workflowType + "' and qm.created_on < '2021-01-25'";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				workflowIds.add(rs.getString("workflow_id"));
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
		System.out.println("Found " + workflowIds.size() + " workflows");
		int totalRecs = workflowIds.size();
		if(workflowIds.size() > 0) {
			for(String workflowId : workflowIds) {
				recsProcessed++;
				try {
					if(this.logEachRecord) {
						System.out.println("[" + new Date().toString() + "] #" + recsProcessed + " terminating workflow " + workflowId);
					}
					terminateWorkflow(workflowId);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] " + workflowType + " processed " + recsProcessed + " out of " + totalRecs;
					System.out.println(msg);
				}
			}
		}
		System.out.println("*******************************************************************");
		System.out.println("For " + workflowType + ", took " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("*******************************************************************");
	}

}