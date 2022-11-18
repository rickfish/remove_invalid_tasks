package com.bcbsfl.es;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmTerminatorByCorrId extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	private PrintWriter fileWriter = null;
	private int totalRecsProcessed = 0;
	private int tasksTerminated = 0, delayTasksTerminated = 0, ackTasksTerminated = 0;
	
	public RpmTerminatorByCorrId() {
	}

	public void terminate() throws Exception {
		String lastCorrelationIdToSkip = null;
		try {
			FileInputStream fis = new FileInputStream(this.outputDirectory + "/rpmTerminateTasksByCorrId_" + this.env + ".txt");
			if(fis != null) {
				BufferedReader fbr = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				String line = fbr.readLine();
				while(line != null) {
					if(StringUtils.isNotBlank(line)) {
						lastCorrelationIdToSkip = line;
					}
					line = fbr.readLine();
				}
				fis.close();
				fbr.close();
			}
		} catch(FileNotFoundException e) {
		}
		String filename = Utils.getProperty("corr.id.file.name");
		if(StringUtils.isBlank(filename)) {
			System.err.println("corr.id.file.name must be specified...exiting");
			return;
		}
		InputStream in = this.getClass().getClassLoader().getResourceAsStream(filename);
		if(in == null) {
			System.err.println("corr.id.file.name of '" + filename + "' could not be found...exiting");
			return;
		}
		try {
			this.fileWriter = new PrintWriter(this.outputDirectory + "/rpmTerminatorByCorrId_" + this.env + ".txt");
		} catch(Exception e) {
			e.printStackTrace();
		}
		List<String> correlationIds = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		String line = br.readLine();
		while(line != null) {
			if(StringUtils.isNotBlank(line)) {
				correlationIds.add(line);
			}
			line = br.readLine();
		}
		br.close();
		in.close();
		System.out.println("Found " + correlationIds.size() + " correlation ids to process");
		long start = System.currentTimeMillis();
		createDataSource();
		if(lastCorrelationIdToSkip != null) {
			System.out.print("....skipping correlation ids until " + lastCorrelationIdToSkip + "...");
		}
		boolean foundStartingPoint = (lastCorrelationIdToSkip == null);
		for(String id : correlationIds) {
			this.totalRecsProcessed++;
			if(foundStartingPoint) {
				if(exists(this.totalRecsProcessed, id)) {
					terminate(this.totalRecsProcessed, id);
				} else {
					if(this.logEachRecord) {
						System.out.println("[" + new Date().toString() + "] #" + this.totalRecsProcessed + " top-level workflow with correlation id " + id + " is not in queue");
					}
				}
			} else {
				if(id.equals(lastCorrelationIdToSkip)) {
					foundStartingPoint = true;
					System.out.println("done skipping, starting to terminate...");
				}
			}
		}
		closeDataSource();
		if(this.fileWriter != null) {
			this.fileWriter.close();
		}
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsProcessed + " correlation ids in " + convertMilliseconds(System.currentTimeMillis() - start));
		new EmailSender().sendResultsEmail(totalRecsProcessed, tasksTerminated, delayTasksTerminated, ackTasksTerminated, System.currentTimeMillis() - start);
	}

	private void terminate(int recordNumber, String correlationId) {
		if(this.logEachRecord) {
			System.out.print("[" + new Date().toString() + "] #" + recordNumber + " correlation id " + correlationId + "...");
		}
		JsonElement je = null;
		JsonObject jo = null;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT message_id, workflow_def, correlation_id, w.workflow_id as the_workflow_id FROM queue_message qm " +
					"INNER JOIN workflow w ON qm.message_id = w.workflow_id " + 
					"INNER JOIN workflow_def_to_workflow wd ON w.workflow_id = wd.workflow_id " +
					"WHERE queue_name = '_deciderQueue' AND w.correlation_id = ? " +
					"ORDER BY w.created_on desc";
			st = con.prepareStatement(query);
			st.setString(1, correlationId);
			rs = st.executeQuery();
			String workflowType = null;
			String workflowId = null;
			String topLevelWorkflowType = null, topLevelWorkflowId = null;
			boolean foundWorkflow = false;
			while(rs.next()) {
				if(foundWorkflow) {
					if(workflowType.startsWith("SSO_RPM")) {
						topLevelWorkflowType = rs.getString("workflow_def");
						topLevelWorkflowId = rs.getString("the_workflow_id");
					}
				} else {
					workflowType = rs.getString("workflow_def");
					if(workflowType.startsWith("SSO_RPM")) {
						workflowId = rs.getString("the_workflow_id");
						foundWorkflow = true;
					} else {
						workflowType = null;
					}
				}
			}
			if(workflowType != null && workflowId != null) {
				if(this.logEachRecord) {
					System.out.println(" workflow " + topLevelWorkflowType + " " + topLevelWorkflowId + "...");
					System.out.print("    most recent subWorkflow " + workflowType + " " + workflowId + "...");
				}
				if(this.logEachRecord) {
				}
				st.close();
				rs.close();
				st = con.prepareStatement("SELECT wtt.task_id as the_task_id, json_data FROM workflow_to_task wtt inner join task t ON t.task_id = wtt.task_id WHERE workflow_id = ?");
				st.setString(1, workflowId);
				rs = st.executeQuery();
				String status = null;
				String taskType = null;
				String taskId = null;
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
								if(this.logEachRecord) {
									System.out.print("taskType " + taskType + " status " + status + "...");
								}
								if("SSO_RPM_DELAY_TASK".equals(taskType) || "SSO_RPM_CHECK_HOME_PLAN_ACK_TASK".equals(taskType)) {
									taskId = rs.getString("the_task_id");
									if(this.logEachRecord) {
										if(this.actuallyPurge) {
											System.out.println("terminating task " + taskId);
										} else {
											System.out.println("if actually.purge was true we would be terminating task " + taskId);
										}
									}
									this.tasksTerminated++;
									if("SSO_RPM_DELAY_TASK".equals(taskType)) {
										this.delayTasksTerminated++;
									} else {
										this.ackTasksTerminated++;
									}
									terminateTask(taskId, "FAILED_WITH_TERMINAL_ERROR");
									this.fileWriter.append(correlationId + "\n");
									this.fileWriter.flush();
								}
								break;
							}
						}
					}
				}
				if(taskId == null) {
					System.out.println("For correlation id " + correlationId + ", COULD NOT FIND a valid SSO_RPM task with a valid status...nothing done");
				}
			} else {
				if(this.logEachRecord) {
					System.out.println("For correlation id " + correlationId + ", COULD NOT FIND an SSO_RPM workflow with that correlation id...nothing done");
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

	private boolean exists(int recordNumber, String correlationId) {
		boolean exists = false;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT count(*) as the_count FROM queue_message qm " + 
					"INNER JOIN workflow_def_to_workflow wd ON qm.message_id = wd.workflow_id " + 
					"INNER JOIN workflow w ON wd.workflow_id = w.workflow_id " + 
					"WHERE queue_name = '_deciderQueue' AND workflow_def IN('SSO_RPM_CODING_GAP_WF','SSO_RPM_CODING_GAP_WF','SSO_RPM_MEDICAL_RECORD_REQUEST_WF') " + 
					"AND w.correlation_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, correlationId);
			rs = st.executeQuery();
			if(rs.next() && rs.getInt("the_count") > 0) {
				exists = true;
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
		return exists;
	}
}