package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CdtClearWorkflows extends TaskPurgerApp {
	private static final String workflowName = "CDT_CDT_SIEBEL_SR_WF";
	private JsonParser jsonParser = new JsonParser();
	
	public CdtClearWorkflows() {
	}

	public void clear() throws Exception {
		long start = System.currentTimeMillis();
		createDataSource();
		long totalRecsForAllQueries = getTotalRecs();
		System.out.println("Getting records to process for all queries");
		System.out.println("Total records to process for all queries: " + totalRecsForAllQueries);
		List<String> workflowIds = null;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		int totalRecsProcessed = 0;
		int offset = this.rowOffset;
		java.sql.Timestamp firstCreatedOn = null, lastCreatedOn = null;
		String lastMessageId = null;
		int waitTasksCompleted = 0, totalWaitTasksCompleted = 0;
		int workflowsDecided = 0, totalWorkflowsDecided = 0;
		while(true) {
			waitTasksCompleted = 0;
			workflowsDecided = 0;
			System.out.println("Getting total records to process");
			recsProcessed = 0;
			workflowIds = new ArrayList<String>();
			try {
				con = getDatabaseConnection();
				if(con == null) {
					throw new Exception("Could not get database connection");
				}
				con.setAutoCommit(false);
				st = con.createStatement();
		
				// Turn use of the cursor on.
				st.setFetchSize(100);

				String query = "select message_id, qm.created_on as create_ts from queue_message qm inner join workflow_def_to_workflow wd on wd.workflow_id = qm.message_id where queue_name = '_deciderQueue' and workflow_def = '" + workflowName + "'";
				if(this.rowLimit > 0) {
					query += (" order by qm.created_on limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("QUERY: " + query);

				rs = st.executeQuery(query);
				int recsAdded = 0;
				while(rs.next()) {
					if(recsAdded == 0) {
						firstCreatedOn = rs.getTimestamp("create_ts");
					}
					lastCreatedOn = rs.getTimestamp("create_ts");
					lastMessageId = rs.getString("message_id");
					workflowIds.add(rs.getString("message_id"));
					if(++recsAdded  % 1000 == 0) {
						System.out.println("So far added " + recsAdded + " to the list of messages to process");
					}
				}
				if(recsAdded == 0) {
					System.out.println("DONE");
					break;
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
			int totalRecs = workflowIds.size();
			System.out.println("Found " + totalRecs + " workflow ids. First created_on " + firstCreatedOn.toString() + ", last created_on " + lastCreatedOn + ", last messageId " + lastMessageId);
			if(totalRecs > 0) {
				try {
					for(String workflowId : workflowIds) {
						recsProcessed++;
						totalRecsProcessed++;
						try {
							if(this.logEachRecord) {
								System.out.print("[" + new Date().toString() + "] Record #" + recsProcessed + ", workflowId " + workflowId + "...checking tasks...");
							}
							ProcessResult result = processWorkflow(workflowId);
							if(result.waitTaskCompleted) {
								waitTasksCompleted++;
								totalWaitTasksCompleted++;
							} else {
								if(result.workflowDecided) {
									workflowsDecided++;
									totalWorkflowsDecided++;
								}							
							}
								
							if(this.logEachRecord) {
								if(result.waitTaskCompleted) {
									System.out.println("WAIT task completed");
								} else if(result.workflowDecided) {
									System.out.println("workflow decided");
								} else { 							
									System.out.println("nothing done");
								}
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " for this query out of " + totalRecs + ", " + totalRecsProcessed + " processed for all queries out of " + totalRecsForAllQueries;
							if(this.rowLimit > 0) {
								msg += " starting at offset " + offset;
							}
							System.out.println(msg);
							System.out.println("    WAIT tasks completed this query: " + waitTasksCompleted + ", total for all queries: " + totalWaitTasksCompleted 
								+ ", workflows decided this query: " + workflowsDecided + ", total for all queries: " + totalWorkflowsDecided);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			String message = Utils.getProperty("which.app") + ": completed processing " + recsProcessed + " queue messages";
			System.out.println(message);
			if(this.rowLimit > 0) {
				offset += this.rowLimit;
			} else {
				break;
			}
		}
		closeDataSource();
		new EmailSender().sendResultsEmail(workflowName, totalRecsProcessed, totalWaitTasksCompleted, totalWorkflowsDecided, System.currentTimeMillis() - start);
	}

	private ProcessResult processWorkflow(String workflowId) {
		ProcessResult result = new ProcessResult();
		String json = null;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT json_data FROM workflow_to_task wtt INNER JOIN task t ON t.task_id = wtt.task_id WHERE workflow_id  = ?";
			st = con.prepareStatement(query);
			st.setString(1, workflowId);
			rs = st.executeQuery();
			JsonElement je = null;
			JsonObject jo = null;
			String taskId = null;
			while(rs.next()) {
				json = rs.getString("json_data");
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je =  jo.get("taskType");
				if(je != null && "WAIT".equals(je.getAsString())) {
					je = jo.get("status");
					if(je != null) {
						if("IN_PROGRESS".equals(je.getAsString())) {
							je = jo.get("taskId");
							if(je != null) {
								taskId = je.getAsString();
							}
							terminateTask(taskId, "COMPLETED");
							result.waitTaskCompleted = true;
							break;
						}
					}
				} else {
					if(je != null && "CDT_CDT_CREATE_SIEBEL_SR_TASK".equals(je.getAsString())) {
						je = jo.get("status");
						if(je != null) {
							if("IN_PROGRESS".equals(je.getAsString()) || "SCHEDULED".equals(je.getAsString())) {
								makeDecideCall(workflowId);
								result.workflowDecided = true;
								break;
							}
						}
					}
				}
			}
			if(result.waitTaskCompleted) {
			} else if(result.workflowDecided) {
				System.out.println("********************** WORKFLOW " + workflowId + " DOES NOT HAVE A WAIT TASK but does have a CDT_CDT_CREATE_SIEBEL_SR_TASK");
			} else {
				System.out.println("********************** WORKFLOW " + workflowId + " DOES NOT HAVE A WAIT TASK and also no CDT_CDT_CREATE_SIEBEL_SR_TASK");
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
		
		return result;
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
	
			String query = "SELECT count(*) as total FROM queue_message qm inner join workflow_def_to_workflow wd on wd.workflow_id = qm.message_id where queue_name = '_deciderQueue' and workflow_def = '" +  workflowName + "'";
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
	
	private class ProcessResult {
		boolean waitTaskCompleted = false;
		boolean workflowDecided = false;
	}
}
