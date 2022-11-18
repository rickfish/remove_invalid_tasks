package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RpmWorkflowTerminator extends TaskPurgerApp {
	public RpmWorkflowTerminator() {
	}

	public void terminate() throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		List<WorkflowInfo> workflows = new ArrayList<WorkflowInfo>(); 
		createDataSource();
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			// Turn use of the cursor on.
			st.setFetchSize(100);

			String query = "SELECT workflow_def, message_id FROM queue_message qm INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id WHERE workflow_def LIKE 'SSO_RPM_%'";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				WorkflowInfo wi = new WorkflowInfo();
				wi.workflowName = rs.getString("workflow_def");
				wi.workflowId = rs.getString("message_id");
				workflows.add(wi);
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
		System.out.println("Found " + workflows.size() + " workflows");
		if(workflows.size() > 0) {
			terminateWorkflows(workflows);
		}
		closeDataSource();
	}

	public void terminateWorkflows(List<WorkflowInfo> workflows) throws Exception {
		int recsProcessed = 0;
		int totalRecs = workflows.size();
		try {
			for(WorkflowInfo wi : workflows) {
				recsProcessed++;
				try {
					if(this.logEachRecord) {
						System.out.println("[" + new Date().toString() + "] #" + recsProcessed + " terminating workflow " + wi.workflowName + " " + wi.workflowId);
					}
					terminateWorkflow(wi.workflowId);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] processed " + recsProcessed + " out of " + totalRecs;
					System.out.println(msg);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private class WorkflowInfo {
		public String workflowId = null;
		public String workflowName = null;
	}
}