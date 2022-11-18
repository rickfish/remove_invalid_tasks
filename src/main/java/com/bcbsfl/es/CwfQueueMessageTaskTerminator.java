package com.bcbsfl.es;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CwfQueueMessageTaskTerminator extends TaskPurgerApp {
	static private final String TASKTYPE_EVENT = "convert_ee_to_cwf_info_worker_1.0";
	static private final String TASKTYPE_ERROR = "claims_workflow_error_task_1.0";
	@SuppressWarnings("deprecation")
	private JsonParser jsonParser = new JsonParser();
	List<String> eventIdsToTerminate = null;
	private String fileName = null;
	public CwfQueueMessageTaskTerminator() {
		this.fileName = Utils.getProperty("file.name");
	}

	public void terminate() throws Exception {
		if(StringUtils.isBlank(this.fileName)) {
			throw new Exception("file.name property cannot be blank");
		}
		this.eventIdsToTerminate = getEventIds();
		if(eventIdsToTerminate.size() == 0) {
			throw new Exception("No event ids were found in the '" + this.fileName + "' file. Nothing to process.");
		} else {
			System.out.println("There are " + eventIdsToTerminate.size() + " event ids in the '" + this.fileName + "' file");
		}
		
		terminateTasksIfNecessary(1, TASKTYPE_EVENT);
		terminateTasksIfNecessary(2, TASKTYPE_ERROR);
	}

	public void terminateTasksIfNecessary(int phase, String taskType) {
		System.out.println("***********************************************************");
		System.out.println("Processing " + taskType + " tasks");
		System.out.println("***********************************************************");

		List<String> taskIds = getTaskIds(taskType);
		System.out.println("Found " + taskIds.size() + " tasks");
		int recCount = 0;
		int totalTasks = taskIds.size();
		int tasksTerminated = 0;
		if(totalTasks > 0) {
			for(String taskId : taskIds) {
				try {
					TaskInfo ti  = TASKTYPE_EVENT.equals(taskType) ?
						getTaskInfoFromCorrelationId(taskId) : getTaskInfoFromEventId(taskId);
					if(this.logEachRecord || this.logOnlyPurged) {
						System.out.print("[" + new Date().toString() + "] Phase " + phase + " #" + (++recCount) + " out of " + totalTasks + ": ");
					}
					if(ti.correlationId == null) {
						if(this.logEachRecord || this.logOnlyPurged) {
							System.out.print("NOT Terminating task " + taskId + " because it does not have a correlationId");
						}
					} else {
						if("SCHEDULED".equals(ti.status) && eventIdsToTerminate.contains(ti.correlationId)) {
							if(this.logEachRecord || this.logOnlyPurged) {
								System.out.print("Terminating task " + taskId + " with correlationId " + ti.correlationId);
							}
							terminateTask(taskId, "FAILED_WITH_TERMINAL_ERROR");
							tasksTerminated++;
						} else {
							if(this.logEachRecord || this.logOnlyPurged) {
								System.out.print("NOT Terminating task " + taskId + " with correlationId " + ti.correlationId);
								if(!"SCHEDULED".equals(ti.status)) {
									System.out.print(" because status is " + ti.status);
								} else {
									System.out.print(" because it is not in the event id list");
								}
							}
						}
					}
					if(this.logEachRecord || this.logOnlyPurged) {
						System.out.println("...so far terminated " + tasksTerminated + " tasks");
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Terminated " + tasksTerminated + " " + taskType + " tasks");

	}
	
	public List<String> getTaskIds(String taskType) {
		List<String> taskIds = new ArrayList<String>();
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		createDataSource();
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
		
			// Turn use of the cursor on.
			st.setFetchSize(100);

			String query = "SELECT message_id FROM queue_message where queue_name = '" + taskType + "'";
			System.out.println("QUERY: " + query);

			rs = st.executeQuery(query);
			while(rs.next()) {
				taskIds.add(rs.getString("message_id"));
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

		closeDataSource();
		return taskIds;
	}

	@SuppressWarnings("deprecation")
	private TaskInfo getTaskInfoFromCorrelationId(String taskId) {
		TaskInfo ti = new TaskInfo();
		String json = this.getTaskJson(taskId);
		if(StringUtils.isNotBlank(json)) {
			JsonElement je = null;
			JsonObject jo = null;
			try {
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je = jo.get("correlationId");
				if(je != null) {
					ti.correlationId = je.getAsString();
				}
				je = jo.get("status");
				if(je != null) {
					ti.status = je.getAsString();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return ti;
	}

	@SuppressWarnings("deprecation")
	private TaskInfo getTaskInfoFromEventId(String taskId) {
		TaskInfo ti = new TaskInfo();
		String json = this.getTaskJson(taskId);
		if(StringUtils.isNotBlank(json)) {
			JsonElement je = null;
			JsonObject jo = null;
			try {
				je = this.jsonParser.parse(json);
				jo = je.getAsJsonObject();
				je = jo.get("status");
				if(je != null) {
					ti.status = je.getAsString();
				}
				je = jo.get("inputData");
				if(je != null) {
					jo = je.getAsJsonObject();
					if(jo != null) {
						je = jo.get("eventId");
						if(je != null && !je.isJsonNull()) {
							ti.correlationId = je.getAsString();
						}
					}
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return ti;
	}

	private List<String> getEventIds() {
		List<String> eventIds = new ArrayList<String>();
		try {
			FileInputStream fis = new FileInputStream(this.fileName);
			if(fis != null) {
				BufferedReader fbr = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				String line = fbr.readLine();
				while(line != null) {
					if(StringUtils.isNotBlank(line)) {
						eventIds.add(line.trim());
					}
					line = fbr.readLine();
				}
				fis.close();
				fbr.close();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return eventIds;
	}
	
	private class TaskInfo {
		public String correlationId = null;
		public String status = null;
	}
}