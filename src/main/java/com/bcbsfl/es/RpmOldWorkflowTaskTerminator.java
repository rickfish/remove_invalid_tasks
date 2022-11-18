package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmOldWorkflowTaskTerminator extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	private int totalRecsProcessed = 0;
	private long totalRecsForAllTaskTypes = 0;
	private Date medrecTerminateBefore = null, caregapTerminateBefore = null, codingGapTerminateBefore = null;
	private int workflowsTerminated = 0, medicalRecordsWorkflowsTerminated = 0, careGapWorkflowsTerminated = 0, codingGapWorkflowsTerminated = 0;
	
	public RpmOldWorkflowTaskTerminator() {
	}

	public void terminate() throws Exception {
		Date d = getDate("medrec.terminate.before");
		if(d != null) {
			this.medrecTerminateBefore = d;
		} else {
			return;
		}
		d = getDate("caregap.terminate.before");
		if(d != null) {
			this.caregapTerminateBefore = d;
		} else {
			return;
		}
		d = getDate("codinggap.terminate.before");
		if(d != null) {
			this.codingGapTerminateBefore = d;
		} else {
			return;
		}
		System.out.println("Terminating old SSO_RPM_MEDICAL_RECORD_REQUEST_WF workflows whose created_on is before " + this.medrecTerminateBefore.toString());
		System.out.println("Terminating old SSO_RPM_CARE_GAP_WF workflows whose created_on is before " + this.caregapTerminateBefore.toString());
		System.out.println("Terminating old SSO_RPM_CODING_GAP_WF workflows whose created_on is before " + this.codingGapTerminateBefore.toString());
		long start = System.currentTimeMillis();
		createDataSource();
		this.totalRecsForAllTaskTypes = getTotalRecs();
		System.out.println("There are " + this.totalRecsForAllTaskTypes + " total records to process for all task types");
		terminate("SSO_RPM_CHECK_HOME_PLAN_ACK_TASK");
		terminate("SSO_RPM_DELAY_TASK");
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsForAllTaskTypes + " records (for all task types) in " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("    " + this.workflowsTerminated + " workflows terminated");
		System.out.println("    " + this.medicalRecordsWorkflowsTerminated + " medical record request workflows terminated");
		System.out.println("    " + this.careGapWorkflowsTerminated + " care gap workflows terminated");
		System.out.println("    " + this.codingGapWorkflowsTerminated + " coding gap workflows terminated");
		new EmailSender().sendResultsEmail(true, totalRecsProcessed,  workflowsTerminated, medicalRecordsWorkflowsTerminated, careGapWorkflowsTerminated, codingGapWorkflowsTerminated, System.currentTimeMillis() - start);
	}

	@SuppressWarnings("deprecation")
	private Date getDate(String propname) {
		String dateString = Utils.getProperty(propname);
		if(StringUtils.isBlank(dateString)) {
			System.err.println("Missing " + propname + " property. Cannot continue.");
			return null;
		}
		String[] dateParts = dateString.split("-");
		if(dateParts.length != 3) {
			System.err.println(propname + " property of " + dateString + " is not a valid date.. Cannot continue.");
			return null;
		}
		return new Date(Integer.parseInt(dateParts[0]) - 1900, Integer.parseInt(dateParts[1]) - 1,Integer.parseInt(dateParts[2]),0,0,0);
	}
	
	public void terminate(String taskType) throws Exception {
		System.out.println("*******************************************************************");
		System.out.println("Starting review of taks of type " + taskType);
		System.out.println("*******************************************************************");
		long start = System.currentTimeMillis();
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
				totalRecsProcessed++;
				try {
					terminateIfAppropriate(recsProcessed, messageId);
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(recsProcessed % 100 == 0) {
					String msg = "[" + new Date().toString() + "] " + taskType + " processed " + recsProcessed + " out of " + totalRecs + ", total records processed " + totalRecsProcessed + " out of " + totalRecsForAllTaskTypes + " for all task types";
					System.out.println(msg);
				}
			}
		}
		System.out.println("*******************************************************************");
		System.out.println("For " + taskType + ", took " + convertMilliseconds(System.currentTimeMillis() - start));
		System.out.println("*******************************************************************");
	}

	private void terminateIfAppropriate(int recordNumber, String taskId) {
		String json = null;
		JsonElement je = null;
		JsonObject jo = null;
		String taskWorkflowId = null, workflowId = null;
		json = getTaskJson(taskId);
		je = this.jsonParser.parse(json);
		String errorMessage = null;
		if(json != null) {
			je = this.jsonParser.parse(json);
			jo = je.getAsJsonObject();
			je = jo.get("workflowInstanceId");
			if(je != null) {
				taskWorkflowId = je.getAsString();
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
			System.out.println("[" + new Date().toString() + "] #" + recordNumber + " task " + taskId + " workflow " + taskWorkflowId + "...");
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
			String query = "SELECT correlation_id, created_on, json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			String workflowType = null;
			String parentWorkflowId = null;
			Timestamp createdOn = null;
			workflowId = taskWorkflowId;
			String correlationId = null;
			while(workflowId != null) {
				st.setString(1, workflowId);
				rs = st.executeQuery();
				if(rs.next()) {
					json = rs.getString("json_data");
					createdOn = rs.getTimestamp("created_on");
					correlationId = rs.getString("correlation_id");
				}
				if(json != null && createdOn != null) {
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("workflowType");
					if(je != null) {
						workflowType = je.getAsString();
					}
					je = jo.get("parentWorkflowId");
					if(je != null) {
						parentWorkflowId = je.getAsString();
						workflowId = parentWorkflowId;
					} else {
						if(shouldTerminateTask(workflowType, createdOn, correlationId)) {
							if(this.logEachRecord) {
								if(this.actuallyPurge) {
									System.out.println("    created on " + createdOn.toString() + "...terminating task for workflow " + workflowType + " " + workflowId);
								} else {
									System.out.println("    created on " + createdOn.toString() + "...if actually.purge was true we would be terminating task for workflow " + workflowType + " " + workflowId);
								}
							} else if(this.logOnlyPurged) {
								System.out.println("[" + new Date().toString() + "] #" + recordNumber + " task " + taskId + " workflow " + taskWorkflowId);
								if(this.actuallyPurge) {
									System.out.println("    created on " + createdOn.toString() + "...terminating task for workflow " + workflowType + " " + workflowId);
								} else {
									System.out.println("    created on " + createdOn.toString() + "...if actually.purge was true we would be terminating task for workflow " + workflowType + " " + workflowId);
								}
							}
							this.terminateTask(taskId,  "FAILED_WITH_TERMINAL_ERROR");
						} else {
							if(this.logEachRecord) {
								System.out.println("    created on " + createdOn.toString() + " workflow " + workflowType + " " + workflowId + "...NOT TERMINATING task because the createdOn date is too recent");
							}
						}
						workflowId = null;
					}
				} else {
					if(this.logEachRecord) {
						System.out.println("COULD NOT GET WORKFLOW with workflow id " + workflowId);
					}
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

	private boolean shouldTerminateTask(String workflowType, Timestamp createdOn, String correlationId) {
		boolean shouldTerminate = false;
		switch(workflowType) {
		case "SSO_RPM_MEDICAL_RECORD_REQUEST_WF":
			shouldTerminate = createdOn.before(this.medrecTerminateBefore);
			if(shouldTerminate) {
				this.medicalRecordsWorkflowsTerminated++;
			}
			break;
		case "SSO_RPM_CARE_GAP_WF":
			shouldTerminate = createdOn.before(this.caregapTerminateBefore);
			if(shouldTerminate) {
				this.careGapWorkflowsTerminated++;
			}
			break;
		case "SSO_RPM_CODING_GAP_WF":
			shouldTerminate = createdOn.before(this.codingGapTerminateBefore);
			if(shouldTerminate) {
				this.codingGapWorkflowsTerminated++;
			}
			break;
		}
		if(shouldTerminate) {
			this.workflowsTerminated++;
		}
		return shouldTerminate;
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
	
			String query = "SELECT count(*) as total FROM queue_message where queue_name in ('SSO_RPM_DELAY_TASK', 'SSO_RPM_CHECK_HOME_PLAN_ACK_TASK')";
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