package com.bcbsfl.es;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmOldWorkflowAging extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	private PrintWriter fileWriter = null;
	
	private int totalRecsProcessed = 0;
	private long totalRecsForAllTaskTypes = 0;

	public RpmOldWorkflowAging() {
	}

	public void report() throws Exception {
		try {
			this.fileWriter = new PrintWriter(this.outputDirectory + "/rpmWorkflowAging_" + this.env + ".csv");
			this.fileWriter.append("WORKFLOW TYPE,WORKFLOW ID,CORRELATION_ID,CREATED ON,MODIFIED ON,AGE IN DAYS\n");
		} catch(Exception e) {
			e.printStackTrace();
		}
		long start = System.currentTimeMillis();
		createDataSource();
		this.totalRecsForAllTaskTypes = getTotalRecs();
		System.out.println("There are " + this.totalRecsForAllTaskTypes + " total records to process for all task types");
		report("SSO_RPM_CHECK_HOME_PLAN_ACK_TASK");
		report("SSO_RPM_DELAY_TASK");
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsForAllTaskTypes + " records (for all task types) in " + convertMilliseconds(System.currentTimeMillis() - start));
		if(this.fileWriter != null) {
			this.fileWriter.close();
		}
		new EmailSender().sendResultsEmail(totalRecsProcessed, System.currentTimeMillis() - start);
	}

	public void report(String taskType) throws Exception {
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
					reportAging(recsProcessed, messageId);
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

	@SuppressWarnings("deprecation")
	private void reportAging(int recordNumber, String taskId) {
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
			String query = "SELECT correlation_id, created_on, modified_on, json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			String workflowType = null;
			String parentWorkflowId = null;
			Timestamp createdOn = null, modifiedOn = null;
			workflowId = taskWorkflowId;
			String correlationId = null;
			long age = 0;
			while(workflowId != null) {
				st.setString(1, workflowId);
				rs = st.executeQuery();
				if(rs.next()) {
					json = rs.getString("json_data");
					createdOn = rs.getTimestamp("created_on");
					modifiedOn = rs.getTimestamp("modified_on");
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
						correlationId = rs.getString("correlation_id");
						Date createdOnDate = new Date(createdOn.getYear(), createdOn.getMonth(), createdOn.getDate(), 0, 0, 0);
						Date now = new Date();
						now.setHours(0);
						now.setMinutes(0);
						now.setSeconds(0);
						age = ((now.getTime() - createdOnDate.getTime()) / 1000) / 60 / 60 / 24;
						if(this.logEachRecord) {
							System.out.println("    created on " + createdOn.toString() + " workflow " + workflowType + " " + workflowId + " " + age + " days old");
						}
						this.fileWriter.append(workflowType + ",");
						this.fileWriter.append(workflowId + ",");
						this.fileWriter.append(correlationId + ",");
						this.fileWriter.append(stringify(createdOn) + ",");
						this.fileWriter.append(stringify(modifiedOn) + ",");
						this.fileWriter.append(new Long(age).toString());
						this.fileWriter.flush();
						this.fileWriter.append("\n");
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
	
	@SuppressWarnings("deprecation")
	private String stringify(Timestamp ts) {
		return "" + (ts.getMonth() + 1) + "/" + ts.getDate() + "/" + (ts.getYear() + 1900) + " " + ts.getHours() + ":" + ts.getMinutes();
	}
}