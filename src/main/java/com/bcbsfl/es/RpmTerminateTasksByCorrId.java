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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.bcbsfl.mail.EmailSender;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmTerminateTasksByCorrId extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	private Map<String, String> correlationIdTask = new HashMap<String, String>();
	private PrintWriter fileWriter = null;
	private int totalRecsProcessed = 0;
	private int tasksTerminated = 0;
	
	public RpmTerminateTasksByCorrId() {
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
			this.fileWriter = new PrintWriter(this.outputDirectory + "/rpmTerminateTasksByCorrId_" + this.env + ".txt");
		} catch(Exception e) {
			e.printStackTrace();
		}
		createDataSource();
		getCorrelationIdTasks("SSO_RPM_CHECK_HOME_PLAN_ACK_TASK");
		getCorrelationIdTasks("SSO_RPM_DELAY_TASK");
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
		if(lastCorrelationIdToSkip != null) {
			System.out.print("....skipping correlation ids until " + lastCorrelationIdToSkip + "...");
		}
		boolean foundStartingPoint = (lastCorrelationIdToSkip == null);
		for(String id : correlationIds) {
			this.totalRecsProcessed++;
			if(foundStartingPoint) {
				terminate(this.totalRecsProcessed, id);
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
		System.out.println("[" + new Date().toString() + "] " + Utils.getProperty("which.app") + ": completed processing " + this.totalRecsProcessed + " correlation ids, terminated " + this.tasksTerminated + " tasks in " + convertMilliseconds(System.currentTimeMillis() - start));
		new EmailSender().sendResultsEmail(totalRecsProcessed, tasksTerminated, System.currentTimeMillis() - start);
	}

	private void getCorrelationIdTasks(String taskType) {
		System.out.println("Getting all tasks of type " + taskType + " to process...");
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
			String query = "SELECT message_id, json_data FROM queue_message qm inner join task t on qm.message_id = t.task_id where queue_name = ?";
			st = con.prepareStatement(query);
			st.setString(1, taskType);
			rs = st.executeQuery();
			String taskId = null, status = null, correlationId = null;
			while(rs.next()) {
				taskId = rs.getString("message_id");
				je = this.jsonParser.parse(rs.getString("json_data"));
				jo = je.getAsJsonObject();
				je = jo.get("correlationId");
				if(je != null ) {
					correlationId = je.getAsString();
					if(StringUtils.isNotBlank(correlationId)) {
						je = jo.get("status");
						if(je != null ) {
							status = je.getAsString();
							if("IN_PROGRESS".equals(status) || "SCHEDULED".equals(status)) {
								if(null == this.correlationIdTask.get(correlationId)) {
									this.correlationIdTask.put(correlationId, taskId);
								} 
							}
						}
					}
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
		System.out.println("Found " + this.correlationIdTask.size() + " unique correlation ids in all the " + taskType + " tasks");
	}

	private void terminate(int recordNumber, String correlationId) {
		if(this.logEachRecord) {
			System.out.print("[" + new Date().toString() + "] #" + recordNumber + " correlation id " + correlationId + "...");
		}
		String taskId = this.correlationIdTask.get(correlationId);
		if(taskId == null) {
			if(this.logEachRecord) {
				System.out.println("no task found for this correlation id...nothing to do");
			}
		} else {
			this.tasksTerminated++;
			if(this.actuallyPurge) {
				if(this.logEachRecord) {
					System.out.print("terminating...");
				}
				if(this.logOnlyPurged) {
					System.out.print("[" + new Date().toString() + "] #" + recordNumber + " correlation id " + correlationId + "...");
				}
				try {
					terminateTask(this.correlationIdTask.get(correlationId), "FAILED_WITH_TERMINAL_ERROR");
					this.fileWriter.append(correlationId + "\n");
					if(this.logEachRecord) {
						System.out.println("terminated");
					}
					if(this.logOnlyPurged) {
						System.out.println("terminated");
					}
				} catch(Exception e) {
					System.out.println("got an exception:");
					e.printStackTrace();
				}
			} else {
				if(this.logEachRecord) {
					System.out.println("if actually.purge was set to true we would be terminating");
				}
			}
		}
	}
}