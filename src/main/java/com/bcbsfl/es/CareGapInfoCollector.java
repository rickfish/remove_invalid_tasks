package com.bcbsfl.es;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CareGapInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	private int totalOfAllWorkflows = 0;
	private int avgWorkflows = 0;
	private int mostWorkflows = 0;
	private int recordNumber = 0;
	private String taskIdWithMost = null;
	private String workflowIdWithMost = null;
	private Map<String, List<CareGapTaskInfo>> allTasks = new HashMap<String, List<CareGapTaskInfo>>(); 
	private Map<String, MergedCareGapTaskInfo> mergedTasks = new HashMap<String, MergedCareGapTaskInfo>(); 
	
	public CareGapInfoCollector() {
	}

	public void collect() throws Exception {
		long start = System.currentTimeMillis();
		createDataSource();
		collect("SSO_RPM_CHECK_HOME_PLAN_ACK_TASK");
		collect("SSO_RPM_DELAY_TASK");
		mergeAllTaskTypes();
		closeDataSource();
		System.out.println("For all combined task tpes, most workflows " + this.mostWorkflows + " from task " + this.taskIdWithMost + ", workflow " + this.workflowIdWithMost + ", avg " + this.avgWorkflows);
		String message = Utils.getProperty("which.app") + ": completed processing in " + convertMilliseconds(System.currentTimeMillis() - start);
		System.out.println(message);
//		new EmailSender().sendResultsEmail(recsProcessed, taskQueueStatistics, workflowQueueStatistics, System.currentTimeMillis() - start);
	}

	public void collect(String taskType) throws Exception {
		System.out.println("*******************************************************************");
		System.out.println("Starting collection of info for task type " + taskType);
		System.out.println("*******************************************************************");
		this.totalOfAllWorkflows = 0;
		this.avgWorkflows = 0;
		this.mostWorkflows = 0;
		this.taskIdWithMost = null;
		this.workflowIdWithMost = null;
		long start = System.currentTimeMillis();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<CareGapTaskInfo> tasks = new ArrayList<CareGapTaskInfo>(); 
		this.recordNumber = 0;
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

			String query = "select message_id from queue_message where queue_name = '" + taskType + "'";
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
		System.out.println("Found " + messageIds.size() + " messages");
		int totalRecs = messageIds.size();
		if(messageIds.size() > 0) {
			for(String messageId : messageIds) {
				recsProcessed++;
				try {
					CareGapTaskInfo taskInfo = collectTaskStatistics(++recordNumber, totalRecs, messageId);
					reportTaskStatistics(recordNumber, taskInfo);
					tasks.add(taskInfo);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] " + taskType + " processed " + recsProcessed + " out of " + totalRecs + ", average workflows " + this.avgWorkflows;
					System.out.println(msg);
				}
			}
		}
		System.out.println("*******************************************************************");
		System.out.println("For " + taskType + ", most workflows " + this.mostWorkflows + " from task " + this.taskIdWithMost + ", workflow " + this.workflowIdWithMost);
		System.out.println("For " + taskType + ", took " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("*******************************************************************");
		this.allTasks.put(taskType, tasks);
	}

	private void mergeAllTaskTypes() {
		this.mostWorkflows = 0;
		this.allTasks.keySet().forEach(taskType -> {
			List<CareGapTaskInfo> taskCareGaps = this.allTasks.get(taskType);
			taskCareGaps.forEach(careGap -> {
				MergedCareGapTaskInfo mergedInfo = null;
				if(this.mergedTasks.containsKey(careGap.topLevelWorkflowId)) {
					mergedInfo = this.mergedTasks.get(careGap.topLevelWorkflowId);
					mergedInfo.merged = true;
				} else {
					mergedInfo = new MergedCareGapTaskInfo();
					this.mergedTasks.put(careGap.topLevelWorkflowId, mergedInfo);
				}
				mergedInfo.workflowId = careGap.topLevelWorkflowId;
				mergedInfo.workflowType = careGap.topLevelWorkflowType;
				mergedInfo.correlationId = careGap.correlationId;
				mergedInfo.createdOn = careGap.createdOn;
				TaskTypeInfo taskInfo = new TaskTypeInfo();
				taskInfo.taskId = careGap.taskId;
				taskInfo.taskType = taskType;
				taskInfo.totalWorkflows = careGap.getTotalWorkflows();
				mergedInfo.taskTypes.add(taskInfo);
				mergedInfo.subworkflowInfo.putAll(careGap.subworkflowInfo);
				if(mergedInfo.getTotalWorklows() > this.mostWorkflows) {
					this.mostWorkflows = mergedInfo.getTotalWorklows();
					this.workflowIdWithMost = careGap.topLevelWorkflowId;
					this.taskIdWithMost = careGap.taskId;
				}
			});
		});
		System.out.println("*******************************************************************");
		System.out.println("RESULTS OF ALL TASK TYPES. There are " + this.mergedTasks.size() + " top-level workflows.");
		System.out.println("*******************************************************************");
		this.avgWorkflows = 0;
		this.recordNumber = 0;
		this.totalOfAllWorkflows = 0;
		List<MergedCareGapTaskInfo> mergedTasksCollection = new ArrayList<MergedCareGapTaskInfo>();
		for(String key : this.mergedTasks.keySet()) {
			mergedTasksCollection.add(this.mergedTasks.get(key));
		}
		Collections.sort(mergedTasksCollection, new SortMergedTasks());
		PrintWriter fileWriter = null;
		try {
			fileWriter = new PrintWriter(this.outputDirectory + "/careGapWorkflowIds_" + this.env + ".txt");
		} catch(Exception e) {
			e.printStackTrace();
		}
		for(MergedCareGapTaskInfo mergedInfo : mergedTasksCollection) {
			this.totalOfAllWorkflows += mergedInfo.getTotalWorklows();
			this.recordNumber++;
			this.avgWorkflows = this.totalOfAllWorkflows / this.recordNumber;
			String msg = "[" + new Date().toString() + "] #" + recordNumber + " Workflow " + mergedInfo.workflowType 
					+ ", id " + mergedInfo.workflowId + ", total workflows " + mergedInfo.getTotalWorklows() 
					+ ", correlationId " + mergedInfo.correlationId + ", createdOn " + mergedInfo.createdOn.toString() 
					+ (mergedInfo.merged ? ", more than one task type" : "");
			System.out.println(msg);
			if(fileWriter != null) {
				fileWriter.append(msg + "\n");
			}
			if(mergedInfo.getTotalWorklows() == this.mostWorkflows) {
				reportMostMergedWorkflows(mergedInfo.workflowId);
			}
		}
		if(fileWriter != null) {
			fileWriter.close();
		}
		if(this.workflowIdWithMost != null) {
			System.out.println("[" + new Date().toString() + "]  END OF MERGED TASK WORKFLOWS, Workflow with the most sub-workflows: " + this.workflowIdWithMost + ", total workflows " + this.mostWorkflows);
			reportMostMergedWorkflows(this.workflowIdWithMost);
		}
		int limit = mergedTasksCollection.size() > 50 ? 50 : mergedTasksCollection.size(); 
		System.out.println("*******************************************************************");
		System.out.println("TOP " + limit + " top-level workflows");
		System.out.println("*******************************************************************");
		this.recordNumber = 0;
		MergedCareGapTaskInfo mergedInfo = null;
		for(int i = 0; i < limit; i++) {
			mergedInfo = mergedTasksCollection.get(i);
			System.out.println("[" + new Date().toString() + "] Workflow " + mergedInfo.workflowType 
				+ ", id " + mergedInfo.workflowId + ", total workflows " + mergedInfo.getTotalWorklows() 
				+ ", correlationId " + mergedInfo.correlationId + ", createdOn " + mergedInfo.createdOn.toString() 
				+ (mergedInfo.merged ? ", more than one task type" : ""));
		}
	}

	private class SortMergedTasks implements Comparator<MergedCareGapTaskInfo> { 
	    public int compare(MergedCareGapTaskInfo a, MergedCareGapTaskInfo b) { 
	        return b.getTotalWorklows() - a.getTotalWorklows(); 
	    } 
	} 
	
	private void reportMostMergedWorkflows(String workflowId) {
		MergedCareGapTaskInfo mergedInfo = this.mergedTasks.get(workflowId);
		if(mergedInfo.getTotalWorklows() == this.mostWorkflows) {
			System.out.println("    Workflows associated with each task type:");
			mergedInfo.taskTypes.forEach(taskTypeInfo -> {
				System.out.println("        " + taskTypeInfo.taskType + "(" + taskTypeInfo.taskId + "): " + taskTypeInfo.totalWorkflows);
			});
			System.out.println("    SubWorkflows associated with this workflow:");
			mergedInfo.subworkflowInfo.keySet().forEach(keyOfMerged -> {
				System.out.println("        " + keyOfMerged + ": " + mergedInfo.subworkflowInfo.get(keyOfMerged));
			});
		}
	}
	
	private void reportTaskStatistics(int recordNumber, CareGapTaskInfo task) {
		int totalWorkflows = task.getTotalWorkflows();
		if(this.logEachRecord || recordNumber % 100 == 0 || totalWorkflows == this.mostWorkflows) {
			System.out.println("[" + new Date().toString() + "] #" + recordNumber + " Task " + task.taskId + ", wf " + task.topLevelWorkflowType + " wfid " + task.topLevelWorkflowId + ", total workflows " + totalWorkflows + ", most " + this.mostWorkflows + "(" + this.workflowIdWithMost + ")");
		}
		if(totalWorkflows == this.mostWorkflows) {
			System.out.println("Task id: " + task.taskId);
			System.out.println("Top-level workflow type: " + task.topLevelWorkflowType);
			System.out.println("Top-level workflow id: " + task.topLevelWorkflowId);
			System.out.println("Total workflows: " + totalWorkflows);
			for(String key : task.subworkflowInfo.keySet()) {
				System.out.println("    " + key + ": " + task.subworkflowInfo.get(key));
			}
		}
	}
	
	private CareGapTaskInfo collectTaskStatistics(int recordNumber, long totalRecords, String taskId) {
		CareGapTaskInfo taskInfo = new CareGapTaskInfo();
		taskInfo.taskId = taskId;
		String taskJson = getTaskJson(taskId);
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

	private void getWorkflowCount(CareGapTaskInfo taskInfo, String workflowId) {
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
			String query = "SELECT correlation_id, created_on, json_data FROM workflow where workflow_id = ?";
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
						taskInfo.correlationId = rs.getString("correlation_id");
						taskInfo.createdOn = rs.getTimestamp("created_on");
						int totalWorkflows = taskInfo.getTotalWorkflows();
						if(totalWorkflows > this.mostWorkflows) {
							this.mostWorkflows = totalWorkflows;
							this.workflowIdWithMost = workflowId;
							this.taskIdWithMost = taskInfo.taskId;
						}
						this.totalOfAllWorkflows += totalWorkflows;
						this.avgWorkflows = this.totalOfAllWorkflows / recordNumber;
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

	public class CareGapTaskInfo {
		public String taskId = null;
		public String topLevelWorkflowType = null;
		public String topLevelWorkflowId = null;
		public String correlationId = null;
		public java.sql.Timestamp createdOn = null;
		public Map<String, Integer> subworkflowInfo = new HashMap<String, Integer>();
		public int getTotalWorkflows() {
			int totalWorkflows = 0;
			for(String key : this.subworkflowInfo.keySet()) {
				totalWorkflows += this.subworkflowInfo.get(key);
			}
			return totalWorkflows;
		}
	}

	public class MergedCareGapTaskInfo {
		public String workflowType = null;
		public String workflowId = null;
		public String correlationId = null;
		public java.sql.Timestamp createdOn = null;
		public List<TaskTypeInfo> taskTypes = new ArrayList<TaskTypeInfo>();
		public Map<String, Integer> subworkflowInfo = new HashMap<String, Integer>();
		public boolean merged = false;
		public int getTotalWorklows() {
			int totalWorkflows = 0;
			for(TaskTypeInfo taskType : this.taskTypes) {
				totalWorkflows += taskType.totalWorkflows;
			}
			return totalWorkflows;
		}
	}

	public class TaskTypeInfo {
		public String taskType = null;
		public String taskId = null;
		public int totalWorkflows = 0;
	}
}