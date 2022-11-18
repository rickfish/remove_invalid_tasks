package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TaskPurger extends TaskPurgerApp {
	private String taskType = null;
	
	private JsonParser jsonParser = new JsonParser();
	
	public TaskPurger() {
		this.taskType = Utils.getProperty("task.type");
		System.out.println("Purging tasks of type " + taskType);
	}

	public void purgeTasks() throws Exception {
		createDataSource();
		long start = System.currentTimeMillis();
		int recordNumber = 0;
		System.out.println("Getting total records to process");
		long totalRecs = getTotalRecs();
		System.out.println("There are " + totalRecs + " to process");
		int recsProcessed = 0, recsSuccessful = 0, recsProcessedThisLoop = 0;
		@SuppressWarnings("unused")
		int successfulThisLoop = 0, unsuccessfulLoops = 0;
		while(true) {
			List<String> taskIds = new ArrayList<String>();
			recsProcessedThisLoop = 0;
			successfulThisLoop = 0;
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
		
				String query = "SELECT message_id FROM queue_message where queue_name = '" + this.taskType + "' AND popped = true LIMIT 100";
				rs = st.executeQuery(query);
				while(rs.next()) {
					taskIds.add(rs.getString("message_id"));
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
			if(taskIds.size() > 0) {
				try {
					boolean successful = false;
					for(String taskId : taskIds) {
						successful = false;
						recsProcessed++;
						recsProcessedThisLoop++;
						try {
							successful = purgeTask(++recordNumber, taskId);
							if(successful) {
								recsSuccessful++;
								successfulThisLoop++;
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 100 == 0) {
							System.out.println("[" + new Date().toString() + "] Processed " + recsProcessed + " out of " + totalRecs + ", successful: " + recsSuccessful);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(successfulThisLoop == 0) {
				unsuccessfulLoops++;
			}
			if(recsProcessedThisLoop == 0 || recsProcessed > totalRecs /* || unsuccessfulLoops == 10*/) {
				break;
			}
		}
		String message = Utils.getProperty("which.app") + ": completed processing " + recsProcessed + " tasks";
		System.out.println(message);
		closeDataSource();
		done();
		new EmailSender().sendResultsEmail(recsProcessed, recsSuccessful, System.currentTimeMillis() - start);
	}

	private boolean purgeTask(int recordNumber, String taskId) {
		boolean successful = false;
		if(this.logEachRecord) {
			System.out.print("[" + new Date().toString() + "] Record #" + recordNumber + ", taskId " + taskId + "...");
		}
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(true);
			st = con.createStatement();
	
			String query = "SELECT json_data FROM task where task_id = '" + taskId + "'";
			rs = st.executeQuery(query);
			String json = null;
			JsonElement je = null;
			JsonObject jo = null;
			String status = null;
			String referenceTaskName = null;
			if(rs.next()) {
				try {
					json = rs.getString("json_data");
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					JsonElement statusObject = jo.get("status");
					if(statusObject != null) {
						status = statusObject.getAsString();
					}
					JsonElement taskRefObject = jo.get("referenceTaskName");
					if(taskRefObject != null) {
						referenceTaskName = taskRefObject.getAsString();
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(this.logEachRecord) {
				System.out.print("taskStatus: " + status + "...");
			}
			if("TIMED_OUT".equals(status)) {
				String workflowId = null;
				if(this.logEachRecord) {
					System.out.print("taskStatus: TIMED_OUT...");
				}
				JsonElement workflowIdElement = jo.get("workflowInstanceId");
				if(workflowIdElement != null) {
					workflowId = workflowIdElement.getAsString();
				}
				if(workflowId != null) {
					if(this.logEachRecord) {
						System.out.println("workflowId: " + workflowId + "...");
					}
					rs.close();
					status = null;
					query = "SELECT json_data FROM workflow where workflow_id = '" + workflowId + "'";
					rs = st.executeQuery(query);
					if(rs.next()) {
						try {
							json = rs.getString("json_data");
							je = this.jsonParser.parse(json);
							jo = je.getAsJsonObject();
							JsonElement statusObject = jo.get("status");
							if(statusObject != null) {
								status = statusObject.getAsString();
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if("COMPLETED".equals(status)) {
							if(this.logEachRecord) {
								System.out.print("    workflowStatus: COMPLETED...");
							}
							if(this.actuallyPurge) {
								rs.close();
								status = null;
								query = "DELETE FROM task_scheduled where workflow_id = '" + workflowId + "' and task_key like  '" + referenceTaskName + "%'";
								int rowsDeleted = st.executeUpdate(query);
								if(this.logEachRecord) {
									System.out.print("Deleted " + rowsDeleted + " from task_scheduled...");
								}
								rs.close();
								status = null;
								query = "DELETE FROM queue_message where queue_name = '" + this.taskType + "' and message_id = '" + taskId + "'";
								rowsDeleted = st.executeUpdate(query);
								if(this.logEachRecord) {
									System.out.println("Deleted " + rowsDeleted + " from queue_message...done");
								}
							} else {
								if(this.logEachRecord) {
									System.out.println("would delete rows if actually.purge.tasks was set to true");
								}
							}
							successful = true;
						} else {
							if(this.logEachRecord) {
								System.out.println("    workflow status is " + status + "...done");
							}
						}
					} else {
						if(this.logEachRecord) {
							System.out.println("    WORKFLOW NOT FOUND...done");
						}
					}
				} else { 
					if(this.logEachRecord) {
						System.out.println("    NO WORKFLOW ID...done");
					}
				}
			} else {
				if(this.logEachRecord) {
					System.out.println("done");
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
		return successful;
	}

	public long getTotalRecs() throws Exception {
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
	
			String query = "SELECT count(*) as total FROM queue_message where queue_name = '" + this.taskType + "' and popped = true";
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