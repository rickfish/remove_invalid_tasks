package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class WaitTaskInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	public WaitTaskInfoCollector() {
	}

	public void collect() {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<String> taskIds = new ArrayList<String>(); 
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

			String query = "select message_id from queue_message where queue_name = 'WAIT'";
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
		if(taskIds.size() > 0) {
			List<WaitTaskInfo> tasks = new ArrayList<WaitTaskInfo>();
			for(String taskId : taskIds) {
				recsProcessed++;
				try {
					tasks.add(collectTaskInfo(recsProcessed, taskId));
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			reportOnTasks(tasks);
		} else {
			System.out.println("NO TASKS FOUND. NOTHING TO DO.");
		}
		closeDataSource();
	}

	private WaitTaskInfo collectTaskInfo(int recordNumber, String taskId) {
		System.out.println("[" + new Date().toString() + "] #" + recordNumber + " task " + taskId + " collecting...");
		WaitTaskInfo taskInfo = new WaitTaskInfo();
		taskInfo.taskId = taskId;
		String json = getTaskJson(taskId);
		if(json != null) {
			JsonElement je = this.jsonParser.parse(json);
			JsonObject jo = je.getAsJsonObject();
			je = jo.get("workflowInstanceId");
			if(je != null) {
				taskInfo.workflowId = je.getAsString();
				json = this.getWorkflowJson(taskInfo.workflowId);
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je = jo.get("workflowType");
				if(je != null) {
					taskInfo.workflowType = je.getAsString();
				}
				je = jo.get("createTime");
				if(je != null) {
					taskInfo.createdOn = je.getAsLong();
				}
			}
		}
		return taskInfo;
	}
	
	private void reportOnTasks(List<WaitTaskInfo> tasks) {
		Collections.sort(tasks, new Comparator<WaitTaskInfo>() {
			public int compare(WaitTaskInfo a, WaitTaskInfo b) {
				return new Date(a.createdOn).compareTo(new Date(b.createdOn));
			}
		});
		System.out.println("******************************************************");
		System.out.println("WAIT TASK INFO");
		System.out.println("******************************************************");
		tasks.forEach(task -> {
			System.out.println("task " + task.taskId + " workflow " + task.workflowType + " " + task.workflowId + " createdOn " + new Date(task.createdOn).toString());
		});
	}
	
	private class WaitTaskInfo {
		public String taskId = null;
		public String workflowType = null;
		public String workflowId = null;
		public long createdOn = 0;
	}
}