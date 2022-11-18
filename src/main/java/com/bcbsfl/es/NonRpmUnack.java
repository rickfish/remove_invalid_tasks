package com.bcbsfl.es;

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

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NonRpmUnack extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	static private List<String> workflowsToNotUnack = new ArrayList<String>();
	static {
		workflowsToNotUnack.add("SSO_RPM_CHECK_IF_HOME_PLAN_ACK_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CARE_GAP_CHECK_MR_INTERNAL_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CARE_GAP_CHECK_MR_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CARE_GAP_RACE_WF");
		workflowsToNotUnack.add("SSO_RPM_CHECK_IF_SENT_MRM_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CHECK_SENT_MRM_INTERNAL_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CHECK_WAIT_MR_FROM_MRM_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CHECK_WAIT_MR_INTERNAL_LOOP_WF");
		workflowsToNotUnack.add("SSO_RPM_CODING_GAP_RACE_WF");
		workflowsToNotUnack.add("SSO_RPM_MEDICAL_RECORD_REQUEST_RACE_WF");
		workflowsToNotUnack.add("SSO_RPM_MEDICAL_RECORD_REQUEST_SENT_MRM_WF");
	}
	
	public NonRpmUnack() {
	}

	public void unack() throws Exception {
		createDataSource();
		while(true) {
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			System.out.println("[" + new Date().toString() + "] STARTING ANOTHER ITERATION");
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			System.out.println("************************************************************************");
			unackIteration();
			System.out.println("[" + new Date().toString() + "] *********** Waiting for 2 hours for the next run...");
			Thread.sleep(7200000); // 2 hours
		}
	}
	
	public void unackIteration() throws Exception {
		long start = System.currentTimeMillis();
		System.out.println("Getting records to process for all queries");
		long totalRecsForAllQueries = getTotalRecs();
		System.out.println("Total records to process for all queries: " + totalRecsForAllQueries);
		List<QueueMessage> queueMessages = null;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		int totalRecsProcessed = 0;
		int offset = this.rowOffset;
		QueueResults totalQueueResults = new QueueResults();
		Map<String, QueueResults> queueResults = new HashMap<String, QueueResults>();
		while(true) {
			System.out.println("Getting total records to process");
			recsProcessed = 0;
			queueMessages = new ArrayList<QueueMessage>();
			try {
				con = getDatabaseConnection();
				if(con == null) {
					throw new Exception("Could not get database connection");
				}
				con.setAutoCommit(false);
				st = con.createStatement();
		
				// Turn use of the cursor on.
				st.setFetchSize(100);

				String query = "select queue_name, message_id from queue_message where popped = true";
				if(this.rowLimit > 0) {
					query += (" order by deliver_on desc limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("QUERY: " + query);

				rs = st.executeQuery(query);
				int recsAdded = 0;
				QueueMessage qm = null;
				while(rs.next()) {
					qm = new QueueMessage();
					qm.id = rs.getString("message_id");
					qm.queue = rs.getString("queue_name");
					queueMessages.add(qm);
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
			int totalRecs = queueMessages.size();
			System.out.println("Found " + totalRecs + " message ids");
			if(totalRecs > 0) {
				try {
					for(QueueMessage qm : queueMessages) {
						recsProcessed++;
						totalRecsProcessed++;
						try {
							if(this.logEachRecord) {
								System.out.print("[" + new Date().toString() + "] Record #" + recsProcessed + "...");
							}
							QueueResult result = processQueueMessage(qm);
							if(result.queue != null) {
								QueueResults qr = queueResults.get(result.queue);
								if(qr == null) {
									qr = new QueueResults();
									qr.isWorkflow = result.isWorkflow;
									queueResults.put(result.queue, qr);
								}
								if(result.rpmObject) {
									totalQueueResults.rpmObjects++;
									qr.rpmObjects++;
								} else if(result.notInProgress) {
									totalQueueResults.notnProgress++;
									qr.notnProgress++;
								} else if(result.notPopped) {
									totalQueueResults.notPopped++;
									qr.notPopped++;
								} else if(result.unacked) {
									totalQueueResults.unacked++;
									qr.unacked++;
								}
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " for this query out of " + totalRecs + ", " + totalRecsProcessed + " processed for all queries out of " + totalRecsForAllQueries
									 + ", unacked: " + totalQueueResults.unacked + ", rpm: " + totalQueueResults.rpmObjects + ", not popped: " + totalQueueResults.notPopped + ", not in progress(deleted): " + totalQueueResults.notnProgress;
							if(this.rowLimit > 0) {
								msg += " starting at offset " + offset;
							}
							System.out.println(msg);
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
		List<QueueResults> sortedQueueResults = new ArrayList<QueueResults>();
		QueueResults qr = null;
		for(String queueName : queueResults.keySet()) {
			qr = queueResults.get(queueName);
			qr.name = queueName;
			sortedQueueResults.add(qr);
		}
		Collections.sort(sortedQueueResults, new SortQueueResults());
	
		sortedQueueResults.forEach(results -> {
			System.out.println((results.isWorkflow ? "Workflow " + results.name : "Task " + results.name) + ", unacked: " + results.unacked + ", rpm: " + results.rpmObjects + ", not popped: " + results.notPopped+ ", not in progress(deleted): " + results.notnProgress);
		});
		new EmailSender().sendResultsEmail(totalRecsProcessed, totalQueueResults, sortedQueueResults, System.currentTimeMillis() - start);
	}

	private class SortQueueResults implements Comparator<QueueResults> { 
	    public int compare(QueueResults a, QueueResults b) { 
	        return a.name.toUpperCase().compareTo(b.name.toUpperCase()); 
	    } 
	} 

	private QueueResult processQueueMessage(QueueMessage qm) {
		QueueResult result = new QueueResult();
		result.queue = qm.queue;
		result.isWorkflow = "_deciderQueue".equals(qm.queue);
		if(result.isWorkflow) {
			processWorkflow(qm, result);
		} else {
			processTask(qm, result);
		}
		return result;
	}

	private void processWorkflow(QueueMessage qm, QueueResult qr) {
		if(this.logEachRecord) {
			System.out.print("workflow " + qm.id +"...");
		}
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
			String query = "SELECT json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, qm.id);
			rs = st.executeQuery();
			JsonElement je = null;
			JsonObject jo = null;
			int version = 0;
			if(rs.next()) {
				json = rs.getString("json_data");
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je =  jo.get("version");
				if(je != null) {
					version = je.getAsInt();
				}
				je =  jo.get("workflowType");
				if(je != null) {
					qr.queue = je.getAsString();
					if(this.logEachRecord) {
						System.out.print("workflowType " + qr.queue + " version " + version + "...");
					}
					je = jo.get("status");
					if(je != null) {
						if("RUNNING".equals(je.getAsString()) || "PAUSED".equals(je.getAsString())) {
							if(version < 2 && workflowsToNotUnack.contains(qr.queue)) {
								qr.rpmObject = true;
								if(this.logEachRecord) {
									System.out.print("we don't process this workflow type...");
								}
							} else {
								unack(con, qr, qm);
							}
						} else {
							if(this.logEachRecord) {
								System.out.print("status " + je.getAsString() + ", ");
							}
							qr.notInProgress = true;
							deleteQueueMessage(con, qr, qm);
						}
					}
				}
			}
			if(this.logEachRecord) {
				if(qr.notInProgress) {
					System.out.println("NOT RUNNING, deleting");
				} else if(qr.rpmObject) {
					System.out.println("nothing to do");
				} else if(qr.notPopped) {
					System.out.println("NOT POPPED, nothing to do");
				} else {
					System.out.println("unacked");
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


	private void processTask(QueueMessage qm, QueueResult qr) {
		if(this.logEachRecord) {
			System.out.print("task" + qm.id + ", taskType " + qr.queue + "...");
		}
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
			String query = "SELECT json_data FROM task where task_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, qm.id);
			rs = st.executeQuery();
			JsonElement je = null;
			JsonObject jo = null;
			if(rs.next()) {
				json = rs.getString("json_data");
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je = jo.get("status");
				if(je != null) {
					if("IN_PROGRESS".equals(je.getAsString()) || "SCHEDULED".equals(je.getAsString())) {
						unack(con, qr, qm);
					} else {
						qr.notInProgress = true;
						je = jo.get("workflowInstanceId");
						if(je != null) {
							qr.workflowInstanceId = je.getAsString();
						}
						if(qr.workflowInstanceId != null) {
							je = jo.get("referenceTaskName");
							if(je != null) {
								qr.taskReferenceName = je.getAsString();
							}
							st.close();
							rs.close();
							query = "SELECT json_data FROM workflow where workflow_id = ?";
							st = con.prepareStatement(query);
							st.setString(1, qr.workflowInstanceId);
							rs = st.executeQuery();
							if(rs.next()) {
								json = rs.getString("json_data");
								je = this.jsonParser.parse(json);
								jo = je.getAsJsonObject();
								je = jo.get("status");
								if(je != null) {
									if("RUNNING".equals(je.getAsString())) {
										qr.workflowInstanceId = null;
										qr.taskReferenceName = null;
									}
								}
							}
						}
						deleteQueueMessage(con, qr, qm);
					}
				}
			}
			if(this.logEachRecord) {
				if(qr.notInProgress) {
					System.out.println("NOT RUNNING, deleted");
				} else if(qr.rpmObject) {
					System.out.println("nothing to do");
				} else if(qr.notPopped) {
					System.out.println("NOT POPPED, nothing to do");
				} else {
					System.out.println("unacked");
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
	
			String query = "SELECT count(*) as total FROM queue_message where popped = true";
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
