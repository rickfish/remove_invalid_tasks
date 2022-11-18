package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TesWorkflowTerminator extends TaskPurgerApp {
	public TesWorkflowTerminator() {
	}

	public void terminate() throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		List<String> workflows = new ArrayList<String>(); 
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

			String query = "SELECT workflow_def, message_id FROM queue_message qm INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id WHERE queue_name = '_deciderQueue' and workflow_def = 'TES_EIP_TO_PAIS_WORKFLOW'";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				workflows.add(rs.getString("message_id"));
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
		int recCount = 0;
		if(workflows.size() > 0) {
			for(String workflowId : workflows) {
				try {
					if(this.logEachRecord || this.logOnlyPurged) {
						System.out.println("[" + new Date().toString() + "] #" + (++recCount) + " Terminating workflow id " + workflowId);
					}
					terminateWorkflow(workflowId);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		closeDataSource();
	}
}