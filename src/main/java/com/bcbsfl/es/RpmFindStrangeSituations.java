package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmFindStrangeSituations extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	private List<TaskToBeResearched> tasksToBeResearched = new ArrayList<TaskToBeResearched>();
 	
	public RpmFindStrangeSituations() {
	}

	public void find() throws Exception {
		createDataSource();
		findAndEvaluateWorkflows();
		researchTasks();
		closeDataSource();
	}

	public void findAndEvaluateWorkflows() throws Exception {
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		List<String> workflowIds = new ArrayList<String>();
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = 
					"SELECT workflow_def, message_id FROM queue_message qm " + 
					"inner join workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id " + 
					"WHERE queue_name = '_deciderQueue' and workflow_def LIKE ('SSO_RPM%')";
			st = con.prepareStatement(query);
			System.out.println("[" + new Date().toString() + "] Executing query '" + query + "' ...");
			rs = st.executeQuery();
			System.out.println("[" + new Date().toString() + "] ...done executing query");
			while(rs.next()) {
				workflowIds.add(rs.getString("message_id"));
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
		int totalRecs = workflowIds.size();
		System.out.println("Found " + totalRecs + " workflows");
		int recnum = 0;
		for(String id : workflowIds) {
			evaluateWorkflow(totalRecs, ++recnum, id);
		}
	}

	private void evaluateWorkflow(int totalRecs, int recordNumber, String workflowId) {
		if(recordNumber % 100 == 0) {
			System.out.println("Processed " + recordNumber + " workflows out of " + totalRecs);
		}
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		JsonElement je = null;
		JsonObject jo = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.prepareStatement("SELECT wtt.task_id as the_task_id, json_data FROM workflow_to_task wtt inner join task t ON t.task_id = wtt.task_id WHERE workflow_id = ?");
			st.setString(1, workflowId);
			rs = st.executeQuery();
			String status = null;
			String taskType = null;
			String taskId = null;
			while(rs.next()) {
				taskId = rs.getString("the_task_id");
				je = this.jsonParser.parse(rs.getString("json_data"));
				jo = je.getAsJsonObject();
				je = jo.get("status");
				if(je != null ) {
					status = je.getAsString();
				}
				je = jo.get("taskType");
				if(je != null ) {
					taskType = je.getAsString();
				}
				boolean needsResearch = false;
				if("SUB_WORKFLOW".equals(taskType)) {
					if("SCHEDULED".equals(status)) {
						needsResearch = true;
						System.out.println("workflow id " + workflowId + " task id " + taskId + " SUB_WORKFLOW in SCHEDULED status");
					}
				} else {
					if("SIMPLE".equals(taskType)) {
						if("SCHEDULED".equals(status)) {
							needsResearch = true;
							System.out.println("workflow id " + workflowId + " task id " + taskId + " SIMPLE TASK in SCHEDULED status");
						}
					}
				}
				if(needsResearch) {
					TaskToBeResearched t = new TaskToBeResearched();
					t.workflowId = workflowId;
					t.taskType = taskType;
					t.taskId = taskId;
					this.tasksToBeResearched.add(t);
				}
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

	private void researchTasks() {
		System.out.println("********************************************************************************");
		System.out.println("STARTING RESEARCH");
		System.out.println("********************************************************************************");
		this.tasksToBeResearched.forEach(t -> researchTask(t));
	}
	
	private void researchTask(TaskToBeResearched taskInfo) {
		JsonElement je = null;
		JsonObject jo = null;
		String json = getTaskJson(taskInfo.taskId);
		String status = null;
		if(json != null) {
			je = this.jsonParser.parse(json);
			jo = je.getAsJsonObject();
			je = jo.get("status");
			if(je != null ) {
				status = je.getAsString();
			}
			if(status == null) {
				System.err.println("COULD NOT GET TASK STATUS FOR TASK TYPE " + taskInfo.taskType + " TASK ID " + taskInfo.taskId);
			}
			if("SUB_WORKFLOW".equals(taskInfo.taskType)) {
				if("SCHEDULED".equals(status)) {
					System.out.println("workflow id " + taskInfo.workflowId + " task id " + taskInfo.taskId + " SUB_WORKFLOW in SCHEDULED status");
				}
			} else {
				if("SCHEDULED".equals(status)) {
					checkSimpleTask(taskInfo);
				}
			}
		} else {
			System.err.println("COULD NOT GET TASK JSON FOR TASK TYPE " + taskInfo.taskType + " TASK ID " + taskInfo.taskId);
		}
	}
	
	private void checkSimpleTask(TaskToBeResearched taskInfo) {
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.prepareStatement("SELECT message_id from queue_message where queue_name = ? and message_id = ?");
			st.setString(1, taskInfo.taskType);
			st.setString(2, taskInfo.taskId);
			rs = st.executeQuery();
			if(!rs.next()) {
				System.out.println("workflow id " + taskInfo.workflowId + " task id " + taskInfo.taskId + " task type " + taskInfo.taskType + " SIMPLE TASK in SCHEDULED status");
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
	
	private class TaskToBeResearched {
		public String workflowId = null;
		public String taskType = null;
		public String taskId = null;
	}
}