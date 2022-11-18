package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class QueueMessagePurger extends TaskPurgerApp {
	private String taskType = null;
	
	private JsonParser jsonParser = new JsonParser();
	
	public QueueMessagePurger() {
		this.taskType = Utils.getProperty("task.type");
		System.out.println("Purging tasks of type " + taskType);
	}

	public void purgeQueueMessages() throws Exception {
		List<QueueMessageInfo> queueMessages = null;
		createDataSource();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recordNumber = 0;
		int recsPurged = 0, totalPurged = 0;
		int recsProcessed = 0;
		int offset = this.rowOffset;
		while(true) {
			long start = System.currentTimeMillis();
			recordNumber = 0;
			System.out.println("Getting total records to process");
			recsPurged = 0;
			recsProcessed = 0;
			queueMessages = new ArrayList<QueueMessageInfo>();
			try {
				con = getDatabaseConnection();
				if(con == null) {
					throw new Exception("Could not get database connection");
				}
				con.setAutoCommit(false);
				st = con.createStatement();
		
				// Turn use of the cursor on.
				st.setFetchSize(100);

				String query = "select queue_name, message_id from queue_message";
				if(StringUtils.isNotBlank(this.taskType)) {
					query += " where queue_name = '" + this.taskType + "'";
				}
				if(this.rowLimit > 0) {
					query += (" order by deliver_on limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("QUERY: " + query);

				rs = st.executeQuery(query);
				QueueMessageInfo queueMessageInfo = null;
				int recsAdded = 0;
				while(rs.next()) {
					queueMessageInfo = new QueueMessageInfo();
					queueMessageInfo.queueName = rs.getString("queue_name");
					queueMessageInfo.messageId = rs.getString("message_id");
					queueMessages.add(queueMessageInfo);
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
			System.out.println("Found " + queueMessages.size() + " queue messages");
			int totalRecs = queueMessages.size();
			if(queueMessages.size() > 0) {
				try {
					boolean purged = false;
					for(QueueMessageInfo queueMessage : queueMessages) {
						purged = false;
						recsProcessed++;
						try {
							purged = purgeQueueMessage(++recordNumber, totalRecs, queueMessage);
							if(purged) {
								recsPurged++;
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 100 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " out of " + totalRecs;
							if(this.rowLimit > 0) {
								msg += " starting at offset " + offset;
								msg += ", purged this run " + recsPurged + ", total purged " + (totalPurged + recsPurged);
							} else {
								msg += ", purged: " + recsPurged;
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
			new EmailSender().sendResultsEmail(recsProcessed, this.rowLimit, offset, recsPurged, totalPurged, System.currentTimeMillis() - start);
			if(this.rowLimit > 0) {
				offset += this.rowLimit;
				offset -= recsPurged;
				totalPurged += recsPurged;
			} else {
				break;
			}
		}
		closeDataSource();
	}

	private boolean purgeQueueMessage(int recordNumber, long totalRecords, QueueMessageInfo queueMessage) {
		if("_deciderQueue".equals(queueMessage.queueName)) {
			return purgeQueueWorkflow(recordNumber, totalRecords, queueMessage);
		} else {
			return purgeQueueTask(recordNumber, totalRecords, queueMessage);
		}
	}

	private boolean purgeQueueTask(int recordNumber, long totalRecords, QueueMessageInfo queueMessage) {
		boolean purged = false;
		String taskId = queueMessage.messageId;
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
			String taskType = null;
			if(rs.next()) {
				try {
					json = rs.getString("json_data");
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("status");
					if(je != null) {
						status = je.getAsString();
					}
					je = jo.get("taskType");
					if(je != null) {
						taskType = je.getAsString();
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(this.logEachRecord) {
				System.out.print("[" + new Date().toString() + "] Record #" + recordNumber + ", taskType " + taskType + ", taskId " + taskId + " taskStatus " + status + "...");
			}
			if("TIMED_OUT".equals(status)) {
				purged = true;
				if(this.actuallyPurge) {
					rs.close();
					query = "DELETE FROM queue_message where queue_name = '" + taskType + "' and message_id = '" + taskId + "'";
					int rowsDeleted = st.executeUpdate(query);
					if(rowsDeleted == 0) {
						purged = false;
					}
					if(this.logOnlyPurged) {
						System.out.print("[" + new Date().toString() + "] Record #" + recordNumber + ", taskType " + taskType + ", taskId " + taskId + " taskStatus " + status + "...");
						System.out.println("\n    Deleted " + rowsDeleted + " from queue_message...done");
					}
					if(this.logEachRecord) {
						System.out.println("\n    Deleted " + rowsDeleted + " from queue_message...done");
					}
				} else {
					if(this.logEachRecord) {
						System.out.println("would delete rows if actually.purge was set to true");
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
		return purged;
	}

	private boolean purgeQueueWorkflow(int recordNumber, long totalRecords, QueueMessageInfo queueMessage) {
		boolean purged = false;
		String workflowId = queueMessage.messageId;
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
	
			String query = "SELECT json_data FROM workflow where workflow_id = '" + workflowId + "'";
			rs = st.executeQuery(query);
			String json = null;
			JsonElement je = null;
			JsonObject jo = null;
			String status = null;
			String workflowType = null;
			if(rs.next()) {
				try {
					json = rs.getString("json_data");
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("status");
					if(je != null) {
						status = je.getAsString();
					}
					je = jo.get("workflowType");
					if(je != null) {
						workflowType = je.getAsString();
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(this.logEachRecord) {
				System.out.print("[" + new Date().toString() + "] Record #" + recordNumber + ", workflowType " + workflowType + ", workflowId " + workflowId + " workflowStatus " + status + "...");
			}
			if("TIMED_OUT".equals(status) || "FAILED".equals(status)) {
				purged = true;
				if(this.actuallyPurge) {
					rs.close();
					status = null;
					query = "DELETE FROM queue_message where queue_name = '_deciderQueue' and message_id = '" + workflowId + "'";
					int rowsDeleted = st.executeUpdate(query);
					if(rowsDeleted == 0) {
						purged = false;
					}
					if(this.logOnlyPurged) {
						System.out.print("[" + new Date().toString() + "] Record #" + recordNumber + ", workflowType " + workflowType + ", workflowId " + workflowId + " workflowStatus " + status + "...");
						System.out.println("\n    Deleted " + rowsDeleted + " from queue_message...done");
					}
					if(this.logEachRecord) {
						System.out.println("\n    Deleted " + rowsDeleted + " from queue_message...done");
					}
				} else {
					if(this.logEachRecord) {
						System.out.println("would delete rows if actually.purge was set to true");
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
		return purged;
	}

	private class QueueMessageInfo {
		public String queueName = null;
		public String messageId = null;
	}
}