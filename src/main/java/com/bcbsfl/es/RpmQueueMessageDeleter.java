package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.bcbsfl.mail.EmailSender;

public class RpmQueueMessageDeleter extends TaskPurgerApp {
	public RpmQueueMessageDeleter() {
	}

	public void delete() throws Exception {
		List<QueueMessageInfo> queueMessages = null;
		createDataSource();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recordNumber = 0;
		int recsDeleted = 0, totalDeleted = 0;
		int recsProcessed = 0;
		int offset = this.rowOffset;
		while(true) {
			long start = System.currentTimeMillis();
			recordNumber = 0;
			System.out.println("Getting total records to process");
			recsDeleted = 0;
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

				String query = "select queue_name, message_id from queue_message qm " +
						"INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id " + 
						"WHERE qm.queue_name = '_deciderQueue' " +
						"AND wd.workflow_def = 'SSO_RPM_CARE_GAP_CHECK_MR_INTERNAL_LOOP_WF'";
				
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
					boolean deleted = false;
					for(QueueMessageInfo queueMessage : queueMessages) {
						deleted = false;
						recsProcessed++;
						try {
							deleted = deleteQueueMessage(++recordNumber, totalRecs, queueMessage);
							if(deleted) {
								recsDeleted++;
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(recsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " out of " + totalRecs;
							if(this.rowLimit > 0) {
								msg += " starting at offset " + offset;
								msg += ", deleted this run " + recsDeleted + ", total deleted " + (totalDeleted + recsDeleted);
							} else {
								msg += ", deleted: " + recsDeleted;
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
			new EmailSender().sendResultsEmail(recsProcessed, this.rowLimit, offset, recsDeleted, totalDeleted, System.currentTimeMillis() - start);
			if(this.rowLimit > 0) {
				offset += this.rowLimit;
				offset -= recsDeleted;
				totalDeleted += recsDeleted;
			} else {
				break;
			}
		}
		closeDataSource();
	}

	private boolean deleteQueueMessage(int recordNumber, long totalRecords, QueueMessageInfo queueMessage) {
		boolean deleted = false;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(true);
			String query = "delete from queue_message where queue_name = ? and message_Id = ?";
			st = con.prepareStatement(query);
			st.setString(1, queueMessage.queueName);
			st.setString(2, queueMessage.messageId);
			int rowsDeleted = st.executeUpdate();
			deleted = (rowsDeleted > 0);
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
		return deleted;
	}

	private class QueueMessageInfo {
		public String queueName = null;
		public String messageId = null;
	}
}