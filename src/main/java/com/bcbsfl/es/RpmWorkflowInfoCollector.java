package com.bcbsfl.es;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmWorkflowInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	private PrintWriter fileWriter = null;
	private String startingDate = null;
	private boolean includeSubWorkflows = false;
	private int totalSubworkflows = 0;
	public RpmWorkflowInfoCollector() {
	}

	public void collect() throws Exception {
		try {
			this.fileWriter = new PrintWriter(this.outputDirectory + "/rpmWorkflowInfo_" + this.env + ".csv");
			this.fileWriter.append("WORKFLOW TYPE,WORKFLOW ID,CORRELATION_ID,CREATED ON,MODIFIED ON,SUBWORKFLOWS,MOST RECENT SUBWORKFLOW TYPE,SUBWORKFLOW,CREATED ON,DEPENDS ON TASK TYPE,DEPENDS ON TASK ID\n");
		} catch(Exception e) {
			e.printStackTrace();
		}
		long start = System.currentTimeMillis();
		this.startingDate = Utils.getProperty("starting.date");
		this.includeSubWorkflows = Utils.getBooleanProperty("include.subworkflows");
		createDataSource();
		collect("SSO_RPM_CARE_GAP_WF");
		collect("SSO_RPM_CODING_GAP_WF");
		collect("SSO_RPM_MEDICAL_RECORD_REQUEST_WF");
		closeDataSource();
		String message = Utils.getProperty("which.app") + ": found " + this.totalSubworkflows + " sub-workflows, completed processing in " + convertMilliseconds(System.currentTimeMillis() - start);
		System.out.println(message);
		if(this.fileWriter != null) {
			this.fileWriter.close();
		}
	}

	public void collect(String workflowType) throws Exception {
		System.out.println("*******************************************************************");
		System.out.println("Starting collecting of all workflows of type " + workflowType);
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

			String query = "select message_id from queue_message qm inner join workflow_def_to_workflow wd on wd.workflow_id = qm.message_id where queue_name = '_deciderQueue' and workflow_def = '" + workflowType + "'";
			if(StringUtils.isNotBlank(this.startingDate)) {
				query += " and wd.created_on > '" + this.startingDate + "'";
			}
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				workflowIds.add(rs.getString("message_id"));
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
						System.out.println("[" + new Date().toString() + "] #" + recsProcessed + " collecting workflow info for " + workflowId);
					}
					collectWorkflowInfo(recsProcessed, workflowType, workflowId);
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
	
	private void collectWorkflowInfo(int recordNumber, String workflowType, String workflowId) {
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
		
			// Turn use of the cursor on.

			String query = "select created_on, modified_on, json_data, correlation_id from workflow where workflow_id = '" + workflowId + "'"; 
			if(StringUtils.isNotBlank(this.startingDate)) {
				query += " and created_on > '" + this.startingDate + "'";
			}
			rs = st.executeQuery(query);
			JsonElement je = null;
			JsonObject jo = null;
			String status = null,  taskType = null, taskId = null;
			boolean countWorkflow = false;
			if(rs.next()) {
				Timestamp createdOn = rs.getTimestamp("created_on");
				Timestamp modifiedOn = rs.getTimestamp("modified_on");
				String correlationId = rs.getString("correlation_id");
				rs.close();
				int subWorkflowCount = 0;
				Timestamp currentCreatedOn = null, mostRecentCreatedOn = null;
				String mostRecentWorkflowId = null, mostRecentJson = null;
				query = "select workflow_id, created_on,json_data from workflow where correlation_id = '" + correlationId + "' and created_on > '" + createdOn.toString() + "'";
				rs = st.executeQuery(query);
				List<SubWorkflowInfo> subWorkflows = new ArrayList<SubWorkflowInfo>();
				while(rs.next()) {
					countWorkflow = false;
					je = this.jsonParser.parse(rs.getString("json_data"));
					jo = je.getAsJsonObject();
					je = jo.get("status");
					if(je != null ) {
						status = je.getAsString();
					}
					if("RUNNING".equals(status) || "PAUSED".equals(status)) {
						countWorkflow = true;
					}
					if(countWorkflow) {
						subWorkflowCount++;
						this.totalSubworkflows++;
						if(this.includeSubWorkflows) {
							SubWorkflowInfo swi = new SubWorkflowInfo();
							swi.id = rs.getString("workflow_id");
							je = jo.get("workflowType");
							if(je != null) {
								swi.type = je.getAsString();
							}
							subWorkflows.add(swi);
						}
						currentCreatedOn = rs.getTimestamp("created_on");
						if(mostRecentCreatedOn == null || currentCreatedOn.after(mostRecentCreatedOn)) {
							mostRecentCreatedOn = currentCreatedOn;
							mostRecentWorkflowId = rs.getString("workflow_id");
							mostRecentJson = rs.getString("json_data");
						}
					}
				}
				if(subWorkflowCount == 0) {
					mostRecentWorkflowId = workflowId;
				}
				rs.close();
				query = "SELECT wtt.task_id as the_task_id, json_data FROM workflow_to_task wtt inner join task t ON t.task_id = wtt.task_id WHERE workflow_id = '" + mostRecentWorkflowId + "'"; 
				if(StringUtils.isNotBlank(this.startingDate)) {
					query += " and t.created_on > '" + this.startingDate + "'";
				}
				rs = st.executeQuery(query);
				while(rs.next()) {
					je = this.jsonParser.parse(rs.getString("json_data"));
					jo = je.getAsJsonObject();
					je = jo.get("status");
					if(je != null ) {
						status = je.getAsString();
						if("SCHEDULED".equals(status) || "IN_PROGRESS".equals(status)) {
							je = jo.get("taskType");
							if(je != null) {
								taskType = je.getAsString();
								taskId = rs.getString("the_task_id");
								break;
							}
						}
					}
				}
				this.fileWriter.append(workflowType + ",");
				this.fileWriter.append(workflowId + ",");
				this.fileWriter.append(correlationId + ",");
				this.fileWriter.append(stringify(createdOn) + ",");
				this.fileWriter.append(stringify(modifiedOn) + ",");
				this.fileWriter.append(subWorkflowCount + ",");
				if(mostRecentWorkflowId.equals(workflowId)) {
					this.fileWriter.append("<NO SUBWORKFLOWS>,,,");
				} else {
					je = this.jsonParser.parse(mostRecentJson);
					jo = je.getAsJsonObject();
					je = jo.get("workflowType");
					this.fileWriter.append((je == null ? " " : je.getAsString()) + ",");
					this.fileWriter.append(mostRecentWorkflowId + ",");
					this.fileWriter.append(stringify(mostRecentCreatedOn) + ",");
				}
				this.fileWriter.append(taskType + ",");
				this.fileWriter.append(taskId);
				this.fileWriter.append("\n");
				if(subWorkflows.size() > 0) {
					for(int i = 0; i < subWorkflows.size(); i++) {
						SubWorkflowInfo swi = subWorkflows.get(i);
						this.fileWriter.append(" " + "," + " " + "," + " " + "," + " " + "," + " " + ",");
						this.fileWriter.append(i == 0 ? "All subworkflows:" : " ");
						this.fileWriter.append("," + swi.type + "," + swi.id + "\n");
					}
 				}
				this.fileWriter.flush();
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
	
	@SuppressWarnings("deprecation")
	private String stringify(Timestamp ts) {
		return "" + (ts.getMonth() + 1) + "/" + ts.getDate() + "/" + (ts.getYear() + 1900) + " " + ts.getHours() + ":" + ts.getMinutes();
	}
	
	private class SubWorkflowInfo {
		String type = null;
		String id = null;
	}
}