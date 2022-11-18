package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class QueueMessageTaskWorkflowTerminator extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	private long totalRecsForAllTaskTypes = 0;
	private String[] taskTypes = null;
	public QueueMessageTaskWorkflowTerminator() {
	}

	public void terminate() throws Exception {
		String s = Utils.getProperty("task.type");
		if(s != null) {
			taskTypes = s.split(",");
			if(taskTypes != null && taskTypes.length > 0) {
				System.out.println("Processing the following task types:");
				for(int i = 0; i < taskTypes.length; i++) {
					System.out.println("    " + taskTypes[i]);
					taskTypes[i] = taskTypes[i].trim();
				}
			}
			else {
				System.out.println("task.type must be specified");
				return;
			}
		} else {
			System.out.println("task.type must be specified");
			return;
		}
		createDataSource();
//		this.totalRecsForAllTaskTypes = getTotalRecs();
//		System.out.println("There are approximately " + this.totalRecsForAllTaskTypes + " total records to process for all task types");
		terminateAllTasks();
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsForAllTaskTypes + " records (for all task types)");
	}

	public void terminateAllTasks() throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<String> messageIds = new ArrayList<String>();
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			// Turn use of the cursor on.
			st.setFetchSize(100);

			StringBuffer sb = new StringBuffer();
			for(String s : this.taskTypes) {
				if(sb.length() > 0) {
					sb.append(",");
				}
				sb.append("'" + s + "'");
			}
			String query = "SELECT message_id FROM queue_message where queue_name in (" + sb.toString() + ")";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				messageIds.add(rs.getString("message_id"));
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
		this.totalRecsForAllTaskTypes = messageIds.size();
		System.out.println("There are approximately " + this.totalRecsForAllTaskTypes + " total records to process for all task types");
		if(messageIds.size() > 0) {
			for(String messageId : messageIds) {
				recsProcessed++;
				try {
					terminateTaskWorkflow(recsProcessed, messageId);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] processed " + recsProcessed + " out of " + this.totalRecsForAllTaskTypes;
					System.out.println(msg);
				}
			}
		}
	}

	private void terminateTaskWorkflow(int recordNumber, String taskId) {
		String json = null;
		JsonElement je = null;
		JsonObject jo = null;
		String workflowId = null;
		json = getTaskJson(taskId);
		je = this.jsonParser.parse(json);
		String errorMessage = null;
		if(json != null) {
			je = this.jsonParser.parse(json);
			jo = je.getAsJsonObject();
			je = jo.get("workflowInstanceId");
			if(je != null) {
				workflowId = je.getAsString();
			} else {
				errorMessage = "No workflowInstanceId attribute found in task json for task id " + taskId;
			}
		} else {
			errorMessage = "No task found for task id " + taskId;
		}
		if(errorMessage != null) {
			System.out.println(errorMessage);
			return;
		}
		if(this.logEachRecord) {
			System.out.println("[" + new Date().toString() + "] #" + recordNumber + " task " + taskId + " terminating workflow " + workflowId);
		}
		try {
			terminateWorkflow(workflowId, "TerminatedByRickDueToGovindsRequest");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private long getTotalRecs() throws Exception {
		long total = 0;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
			StringBuffer sb = new StringBuffer();
			for(String s : this.taskTypes) {
				if(sb.length() > 0) {
					sb.append(",");
				}
				sb.append("'" + s + "'");
			}
			String query = "SELECT count(*) as total FROM queue_message where queue_name in (" + sb.toString() + ")";
			rs = st.executeQuery(query);
			if(rs.next()) {
				total = rs.getLong("total");
			}
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
		return total;
	}
}