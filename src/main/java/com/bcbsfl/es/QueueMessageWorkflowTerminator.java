package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class QueueMessageWorkflowTerminator extends TaskPurgerApp {
	private String workflowType = null;
	public QueueMessageWorkflowTerminator() {
		this.workflowType = Utils.getProperty("workflow.type");
	}

	public void terminate() throws Exception {
		if(StringUtils.isBlank(this.workflowType)) {
			throw new Exception("workflow.type property cannot be blank");
		}
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		List<QueueMessage> messages = new ArrayList<QueueMessage>(); 
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

			String query = "SELECT workflow_def, message_id FROM queue_message qm " +
					"INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id where queue_name = '_deciderQueue' " +
					" and workflow_def ";
			if(this.workflowType.contains("%")) {
				query += ("like '" + this.workflowType + "'");
			} else {
				query += ("= '" + this.workflowType +"'");
			}
			 
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			QueueMessage qm = null;
			while(rs.next()) {
				qm = new QueueMessage();
				qm.id = rs.getString("message_id");
				qm.queue = rs.getString("workflow_def");
				messages.add(qm);
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
		System.out.println("Found " + messages.size() + " messages");
		int recCount = 0;
		if(messages.size() > 0) {
			for(QueueMessage qm : messages) {
				try {
					if(this.logEachRecord || this.logOnlyPurged) {
						System.out.println("[" + new Date().toString() + "] #" + (++recCount) + " Terminating workflow " + qm.queue + " "  + qm.id);
					}
					terminateWorkflow(qm.id);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		closeDataSource();
	}
}