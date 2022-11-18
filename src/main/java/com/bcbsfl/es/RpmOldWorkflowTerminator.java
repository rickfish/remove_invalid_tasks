package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmOldWorkflowTerminator extends TaskPurgerApp {
	static private List<String> correlationIdsToKeep = new ArrayList<String>();
	static {
		correlationIdsToKeep.add("45701");
		correlationIdsToKeep.add("45728");
		correlationIdsToKeep.add("45722");
		correlationIdsToKeep.add("46630");
		correlationIdsToKeep.add("46635");
		correlationIdsToKeep.add("46633");
		correlationIdsToKeep.add("46636");
		correlationIdsToKeep.add("46634");
	}
	
	private JsonParser jsonParser = new JsonParser();
	
	private long totalRecsForAllWorkflowTypes = 0;
	private String terminateBefore = null;
	private int workflowsTerminated = 0, medicalRecordsWorkflowsTerminated = 0, careGapWorkflowsTerminated = 0, codingGapWorkflowsTerminated = 0;
	
	public RpmOldWorkflowTerminator() {
	}

	public void terminate() throws Exception {
		this.terminateBefore = Utils.getProperty("rpm.terminate.before");
		System.out.println("Terminating old SSO_RPM_MEDICAL_RECORD_REQUEST_WF, SSO_RPM_CARE_GAP_WF, SSO_RPM_CODING_GAP_WF workflows whose created_on is before " + this.terminateBefore);
		long start = System.currentTimeMillis();
		createDataSource();
		this.totalRecsForAllWorkflowTypes = getTotalRecs();
		System.out.println("There are " + this.totalRecsForAllWorkflowTypes + " total records to process for all workflow types");
		terminateWorkflows();
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsForAllWorkflowTypes + " records (for all workflow types) in " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("    " + this.workflowsTerminated + " workflows terminated");
		System.out.println("    " + this.medicalRecordsWorkflowsTerminated + " medical record request workflows terminated");
		System.out.println("    " + this.careGapWorkflowsTerminated + " care gap workflows terminated");
		System.out.println("    " + this.codingGapWorkflowsTerminated + " coding gap workflows terminated");
	}

	public void terminateWorkflows() throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<CorrelationIdInfo> correlationIds = new ArrayList<CorrelationIdInfo>();
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			String query = "select correlation_id, w.workflow_id as the_workflow_id, workflow_def from queue_message qm " +  
					"INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id " + 
					"INNER JOIN workflow w ON wd.workflow_id = w.workflow_id " +
					"WHERE qm.queue_name = '_deciderQueue' " + 
					"AND wd.workflow_def IN ('SSO_RPM_CARE_GAP_WF','SSO_RPM_CODING_GAP_WF','SSO_RPM_MEDICAL_RECORD_REQUEST_WF') " +
					"AND wd.modified_on < '2021-02-18' " +
					"ORDER BY correlation_id";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			CorrelationIdInfo ci = null;
			while(rs.next()) {
				ci = new CorrelationIdInfo();
				ci.workflowId = rs.getString("the_workflow_id");
				ci.correlationId = rs.getString("correlation_id");
				ci.workflowType = rs.getString("workflow_def");
				correlationIds.add(ci);
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
		System.out.println("Found " + correlationIds.size() + " workflows");
		int totalRecs = correlationIds.size();
		if(correlationIds.size() > 0) {
			for(CorrelationIdInfo ci : correlationIds) {
				recsProcessed++;
				try {
					terminateCorrelationId(recsProcessed, ci);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] processed " + recsProcessed + " out of " + totalRecs;
					System.out.println(msg);
				}
			}
		}
	}

	private void terminateCorrelationId(int recordNumber, CorrelationIdInfo ci) {
		List<WorkflowInfo> workflows = new ArrayList<WorkflowInfo>();
		System.out.print("[" + new Date().toString() + "] #" + recordNumber + ": Terminating workflows with correlation id " + ci.correlationId + ", workflow type " + ci.workflowType + "...");
		if(correlationIdsToKeep.contains(ci.correlationId)) {
			System.out.println("");
			System.out.println("    This workflow (workflow id " + ci.workflowId + ") is one of the ones we are not supposed to terminate");
			return;
		}
		this.workflowsTerminated++;
		switch(ci.workflowType) {
		case "SSO_RPM_MEDICAL_RECORD_REQUEST_WF":
			this.medicalRecordsWorkflowsTerminated++;
			break;
		case "SSO_RPM_CARE_GAP_WF":
			this.careGapWorkflowsTerminated++;
			break;
		case "SSO_RPM_CODING_GAP_WF":
			this.codingGapWorkflowsTerminated++;
			break;
		}
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT w.created_on as created_on_ts, w.workflow_id as the_workflow_id, workflow_def, json_data " +
					"FROM workflow w " +
					"INNER JOIN workflow_def_to_workflow wd ON wd.workflow_id = w.workflow_id " +
					"WHERE correlation_id = ? and workflow_def LIKE 'SSO_RPM%' " +
					"ORDER BY w.created_on desc";
			st = con.prepareStatement(query);
			st.setString(1,  ci.correlationId);
			String json = null;
			WorkflowInfo wi = null;
			JsonObject jo = null;
			JsonElement je = null;
			rs = st.executeQuery();
			while(rs.next()) {
				wi = new WorkflowInfo();
				wi.workflowId = rs.getString("the_workflow_id");
				wi.createdOn = rs.getTimestamp("created_on_ts");
				wi.workflowType = rs.getString("workflow_def");
				json = rs.getString("json_data");
				if(json != null) {
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("status");
					if(je != null) {
						wi.status = je.getAsString();
					}
				}
				workflows.add(wi);
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
		System.out.println("" + workflows.size() + " workflows");
		int workflowCount = 0;
		for(WorkflowInfo wi : workflows) {
			System.out.print("    " + (++workflowCount) + ": workflow " + wi.workflowId + ": ");
			if("RUNNING".equals(wi.status)) {
				System.out.print("terminating workflow of type " + wi.workflowType + ", created on " + wi.createdOn + "...");
				try {
					this.terminateWorkflow(wi.workflowId);
					System.out.println("terminated");
				} catch(Exception e) {
					System.out.println("GOT AN ERROR - " + e.getMessage());
				}
			} else {
				System.out.println(" " + wi.workflowType + " workflow, created on " + wi.createdOn + " is " + wi.status + " so it won't be terminated");
			}
		}
	}

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
	
			String query = "SELECT count(*) as total FROM queue_message qm " +  
					"INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id " +
					" WHERE qm.queue_name = '_deciderQueue' " +
					" AND wd.workflow_def IN ('SSO_RPM_CARE_GAP_WF','SSO_RPM_CODING_GAP_WF','SSO_RPM_MEDICAL_RECORD_REQUEST_WF') " +
					" AND wd.modified_on < '" + this.terminateBefore + "'";

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
	
	private class CorrelationIdInfo {
		String correlationId = null;
		String workflowId = null;
		String workflowType = null;
	}		

	private class WorkflowInfo {
		String workflowId = null;
		String workflowType = null;
		String status = null;
		Timestamp createdOn = null;
	}		
}