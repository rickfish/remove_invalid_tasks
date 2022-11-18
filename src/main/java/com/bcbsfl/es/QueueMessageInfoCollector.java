package com.bcbsfl.es;

import java.sql.Connection;
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

public class QueueMessageInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	public QueueMessageInfoCollector() {
	}

	public void collect() throws Exception {
		long start = System.currentTimeMillis();
		createDataSource();
		List<QueueMessageInfo> queueMessages = null;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recordNumber = 0;
		int recsProcessed = 0;
		int offset = this.rowOffset;
		Map<String, Map<String, QueueStatistics>> eventQueueStatistics = new HashMap<String, Map<String, QueueStatistics>>(); 
		Map<String, Map<String, QueueStatistics>> taskQueueStatistics = new HashMap<String, Map<String, QueueStatistics>>(); 
		Map<String, Map<String, QueueStatistics>> workflowQueueStatistics = new HashMap<String, Map<String, QueueStatistics>>(); 
		while(true) {
			recordNumber = 0;
			System.out.println("Getting total records to process");
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
					for(QueueMessageInfo queueMessage : queueMessages) {
						recsProcessed++;
						try {
							collectQueueStatistics(++recordNumber, totalRecs, queueMessage, taskQueueStatistics, workflowQueueStatistics, eventQueueStatistics);
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " out of " + totalRecs;
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
		closeDataSource();
		new EmailSender().sendResultsEmail(recsProcessed, taskQueueStatistics, workflowQueueStatistics, System.currentTimeMillis() - start);
		reportQueueStatistics(recsProcessed, taskQueueStatistics, workflowQueueStatistics, eventQueueStatistics);
	}

	private void collectQueueStatistics(int recordNumber, int totalRecords, QueueMessageInfo queueMessage, 
			Map<String, Map<String, QueueStatistics>> taskQueueStatistics, 
			Map<String, Map<String, QueueStatistics>> workflowQueueStatistics,
			Map<String, Map<String, QueueStatistics>> eventQueueStatistics) {
		if("_deciderQueue".equals(queueMessage.queueName)) {
			collectWorkflowQueueStatistics(recordNumber, totalRecords, queueMessage, workflowQueueStatistics);
		} else if(queueMessage.queueName.contains(":")){
			collectEventQueueStatistics(recordNumber, totalRecords, queueMessage, eventQueueStatistics);
		} else {
			if(!collectTaskQueueStatistics(recordNumber, totalRecords, queueMessage, taskQueueStatistics)) {
				collectEventQueueStatistics(recordNumber, totalRecords, queueMessage, eventQueueStatistics);
			}
		}
	}

	private void reportQueueStatistics(int totalRecords, 
			Map<String, Map<String, QueueStatistics>> taskQueueStatistics, 
			Map<String, Map<String, QueueStatistics>> workflowQueueStatistics,
			Map<String, Map<String, QueueStatistics>> eventQueueStatistics) {
		reportQueueStatistics("Workflow", workflowQueueStatistics);
		reportQueueStatistics("Task", taskQueueStatistics);
		reportQueueStatistics("Event", eventQueueStatistics);
	}

	private void reportQueueStatistics(String queueDescription, Map<String, Map<String, QueueStatistics>> queueStatistics) {
		System.out.println(queueDescription + "s");
		List<QueueToSort> sortedQueues = new ArrayList<QueueToSort>();
		for(String queueName : queueStatistics.keySet()) {
			int messageCount = 0;
			Map<String, QueueStatistics> queueStats = queueStatistics.get(queueName);
			for(String status : queueStats.keySet()) {
				QueueStatistics stats = queueStats.get(status);
				messageCount += stats.count;
			}
			QueueToSort q = new QueueToSort();
			q.queueName = queueName;
			q.count = messageCount;
			sortedQueues.add(q);
		}
		Collections.sort(sortedQueues, new SortQueues());
		sortedQueues.forEach(sortedQueue -> {
			System.out.println("    " + queueDescription + ": " + sortedQueue.queueName + " " + sortedQueue.count);
			Map<String, QueueStatistics> queueStats = queueStatistics.get(sortedQueue.queueName);
			queueStats.keySet().forEach(status  -> {
				QueueStatistics stats = queueStats.get(status);
				System.out.println("        Status: " + status + " had " + stats.count + " messages");
			});
		});
	}
	
	private class SortQueues implements Comparator<QueueToSort> { 
	    public int compare(QueueToSort a, QueueToSort b) { 
	        return b.count - a.count; 
	    } 
	} 
	
	private boolean collectTaskQueueStatistics(int recordNumber, long totalRecords, QueueMessageInfo queueMessage,
			Map<String, Map<String, QueueStatistics>> queueStatistics) {
		boolean foundTask = false;
		String taskId = queueMessage.messageId;
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
	
			String query = "SELECT json_data FROM task where task_id = '" + taskId + "'";
			rs = st.executeQuery(query);
			String json = null;
			JsonElement je = null;
			JsonObject jo = null;
			String status = null;
			String taskType = null;
			if(rs.next()) {
				foundTask = true;
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
					newMessageId(queueMessage.queueName, taskId, status, queueStatistics);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(foundTask) {
				if(this.logEachRecord) {
					System.out.println("[" + new Date().toString() + "] Record #" + recordNumber + ", queueName " + queueMessage.queueName + ", taskType " + taskType + ", taskId " + taskId + " taskStatus " + status + "...collected");
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
		return foundTask;
	}

	private void collectWorkflowQueueStatistics(int recordNumber, long totalRecords, QueueMessageInfo queueMessage,
			Map<String, Map<String, QueueStatistics>> queueStatistics) {
		String workflowId = queueMessage.messageId;
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
					newMessageId(workflowType, workflowId, status, queueStatistics);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(this.logEachRecord) {
				System.out.println("[" + new Date().toString() + "] Record #" + recordNumber + ", workflowType " + workflowType + ", workflowId " + workflowId + " workflowStatus " + status + "...collected");
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

	private void collectEventQueueStatistics(int recordNumber, long totalRecords, QueueMessageInfo queueMessage,
			Map<String, Map<String, QueueStatistics>> queueStatistics) {
		String messageId = queueMessage.messageId;
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
	
			String query = "SELECT json_data FROM event_execution where message_id = '" + messageId + "'";
			rs = st.executeQuery(query);
			String json = null;
			JsonElement je = null;
			JsonObject jo = null;
			String eventType = null;
			if(rs.next()) {
				try {
					json = rs.getString("json_data");
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("name");
					if(je != null) {
						eventType = je.getAsString();
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			} else {
				eventType = "EVENT_NOT_IN_DB";
			}
			newMessageId(eventType, messageId, "STATUS_NOT_APPLICABLE", queueStatistics);
			if(this.logEachRecord) {
				System.out.println("[" + new Date().toString() + "] Record #" + recordNumber + ", queueName " + queueMessage.queueName + ", eventType " + eventType + "...collected");
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

	private void newMessageId(String queueName, String messageId, String status,
			Map<String, Map<String, QueueStatistics>> queueStatistics) {
		QueueStatistics queueStats = null;
		Map<String, QueueStatistics> queueStatsMap = queueStatistics.get(queueName);
		if(queueStatsMap == null) {
			queueStats = new QueueStatistics();
			queueStatsMap = new HashMap<String, QueueStatistics>();
			queueStatsMap.put(status, queueStats);
			queueStatistics.put(queueName, queueStatsMap);
		} else {
			queueStats = queueStatsMap.get(status);
			if(queueStats == null) {
				queueStats = new QueueStatistics();
				queueStatsMap.put(status, queueStats);
			}
		}
		queueStats.newMessageId(this.sampleIdCount, messageId);
	}

	private class QueueMessageInfo {
		public String queueName = null;
		public String messageId = null;
	}
}