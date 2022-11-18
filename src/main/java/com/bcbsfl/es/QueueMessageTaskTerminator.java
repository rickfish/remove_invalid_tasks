package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class QueueMessageTaskTerminator extends TaskPurgerApp {
	private String taskType = null;
	public QueueMessageTaskTerminator() {
		this.taskType = Utils.getProperty("task.type");
	}

	public void terminate() throws Exception {
		if(StringUtils.isBlank(this.taskType)) {
			throw new Exception("task.type property cannot be blank");
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

			String query = "SELECT queue_name, message_id FROM queue_message where queue_name ";
			if(this.taskType.contains("%")) {
				query += ("like '" + this.taskType + "'");
			} else {
				query += ("= '" + this.taskType + "'");
			}
			 
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			QueueMessage qm = null;
			while(rs.next()) {
				qm = new QueueMessage();
				qm.id = rs.getString("message_id");
				qm.queue = rs.getString("queue_name");
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
						System.out.println("[" + new Date().toString() + "] #" + (++recCount) + " Terminating task " + qm.queue + " "  + qm.id);
					}
					terminateTask(qm.id, "FAILED_WITH_TERMINAL_ERROR");
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		closeDataSource();
	}
}