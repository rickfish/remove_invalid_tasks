package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmOneTaskInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	public RpmOneTaskInfoCollector() {
	}

	public void collect() {
		createDataSource();
		TaskInfo taskInfo = collect(Utils.getProperty("task.id"));
		closeDataSource();
		reportTaskStatistics(taskInfo);
	}

	private TaskInfo collect(String taskId) {
		TaskInfo taskInfo = new TaskInfo();
		taskInfo.taskId = taskId;
		String taskJson = getTaskJson(taskInfo.taskId);
		if(taskJson != null) {
			JsonElement je = this.jsonParser.parse(taskJson);
			JsonObject jo = je.getAsJsonObject();
			je = jo.get("workflowInstanceId");
			if(je != null) {
				getWorkflowCount(taskInfo, je.getAsString());
			}
		}
		return taskInfo;
	}

	private void reportTaskStatistics(TaskInfo task) {
		int totalWorkflows = task.getTotalWorkflows();
		System.out.println("[" + new Date().toString() + "] Task " + task.taskId + ", wf " + task.topLevelWorkflowType + " wfid " + task.topLevelWorkflowId + ", total workflows " + totalWorkflows);
		System.out.println("Task id: " + task.taskId);
		System.out.println("Top-level workflow type: " + task.topLevelWorkflowType);
		System.out.println("Top-level workflow id: " + task.topLevelWorkflowId);
		System.out.println("Total workflows: " + totalWorkflows);
		for(String key : task.subworkflowInfo.keySet()) {
			System.out.println("    " + key + ": " + task.subworkflowInfo.get(key));
		}
	}
	
	private void getWorkflowCount(TaskInfo taskInfo, String workflowId) {
		Map<String, Integer> taskWorkflows = taskInfo.subworkflowInfo;
		String workflowJson = null;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			JsonElement je = null;
			JsonObject jo = null;
			String workflowType = null;
			String parentWorkflowId = null;
			while(workflowId != null) {
				st.setString(1, workflowId);
				rs = st.executeQuery();
				if(rs.next()) {
					workflowJson = rs.getString("json_data");
				}
				if(workflowJson != null) {
					je = this.jsonParser.parse(workflowJson);
					jo = je.getAsJsonObject();
					je = jo.get("workflowType");
					if(je != null) {
						workflowType = je.getAsString();
					}
					je = jo.get("parentWorkflowId");
					if(je != null) {
						parentWorkflowId = je.getAsString();
						Integer workflowCount = taskWorkflows.get(workflowType);
						if(workflowCount == null) {
							taskWorkflows.put(workflowType, new Integer(1));
						} else {
							taskWorkflows.put(workflowType, new Integer(workflowCount + 1));
						}
						workflowId = parentWorkflowId;
					} else {
						taskInfo.topLevelWorkflowType = workflowType;
						taskInfo.topLevelWorkflowId = workflowId;
						workflowId = null;
					}
				} else {
					workflowId = null;
				}
				rs.close();
			}
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
	}

	public class TaskInfo {
		public String taskId = null;
		public String topLevelWorkflowType = null;
		public String topLevelWorkflowId = null;
		public Map<String, Integer> subworkflowInfo = new HashMap<String, Integer>();
		public int getTotalWorkflows() {
			int totalWorkflows = 0;
			for(String key : this.subworkflowInfo.keySet()) {
				totalWorkflows += this.subworkflowInfo.get(key);
			}
			return totalWorkflows;
		}
	}
}