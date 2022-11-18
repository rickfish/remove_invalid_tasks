package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
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

public class RemoveOldWorkflows extends TaskPurgerApp {
	private static final String WORKFLOWS_TO_TERMINATE_IF_RUNNING_PROPNAME = "workflows.to.terminate.if.running";
	private static final String TERMINATE_ALL_WORKFLOWS_IF_RUNNING = "ALL";
	private static final String TERMINATE_IF_RUNNING_PROPNAME = "terminate.if.running";
	private static final String PROCESS_ORPHANED_TASKS_PROPNAME = "process.orphaned.tasks";
	private static final int MAX_RUNNING_WORKFLOWS = 1000;
	private static final String BUS_EXCEPTION_WORKFLOW = "EIT_EEB_BUS_EXCEPTION_WF";
	@SuppressWarnings("deprecation")
	private JsonParser jsonParser = new JsonParser();
	private String cutoffDate = Utils.getProperty("starting.date");
	private boolean terminateIfRunning = Utils.getBooleanProperty(TERMINATE_IF_RUNNING_PROPNAME);
	private boolean terminateAllWorkflowsIfRunning = false;
	private List<String> workflowsToTerminateIfRunning = null;
	private boolean processOrphanedTaks = Utils.getBooleanProperty(PROCESS_ORPHANED_TASKS_PROPNAME);
	private List<String> runningWorkflows = new ArrayList<String>();
	private Map<String, Integer> workflowsRunningNotTerminated = new HashMap<String, Integer>();
	private Map<String, InventoryTypeInfo> busExceptionInventoryTypes = new HashMap<String, InventoryTypeInfo>();
	
	public RemoveOldWorkflows() {
		if(this.terminateIfRunning) {
			String workflows = Utils.getProperty(WORKFLOWS_TO_TERMINATE_IF_RUNNING_PROPNAME);
			if(workflows != null && workflows.length() > 0) {
				if(TERMINATE_ALL_WORKFLOWS_IF_RUNNING.equalsIgnoreCase(workflows)) {
					this.terminateAllWorkflowsIfRunning = true;
				} else {
					this.workflowsToTerminateIfRunning = new ArrayList<String>();
					String[] sa = workflows.split(",");
					for(int i = 0; i < sa.length; i++) {
						this.workflowsToTerminateIfRunning.add(sa[i].trim()); 
					}
				}
			}
		}
	}

	public void remove() throws Exception {
		if(StringUtils.isBlank(this.cutoffDate)) {
			throw new Exception("starting.date is the cutoff date and cannot be blank");
		} 
		System.out.println(TERMINATE_IF_RUNNING_PROPNAME + " is set to " + this.terminateIfRunning);
		String workflowsToTerminate = Utils.getProperty(WORKFLOWS_TO_TERMINATE_IF_RUNNING_PROPNAME);
		System.out.println(WORKFLOWS_TO_TERMINATE_IF_RUNNING_PROPNAME + " is set to " + workflowsToTerminate);
		if(this.terminateIfRunning) {
			if(this.terminateAllWorkflowsIfRunning) {
				System.out.println("All types of workflows will be terminated if they are still running");
			} else {
				if(this.workflowsToTerminateIfRunning != null && this.workflowsToTerminateIfRunning.size() > 0) {
					System.out.println("The following are the only types of workflows that will be terminated if they are still running:");
					this.workflowsToTerminateIfRunning.forEach(workflow -> {
						System.out.println("    " + workflow);
					});
				}
			}
		}
		System.out.println("starting.date is set to " + this.cutoffDate + " so we will remove workflows before that date");
		createDataSource();
		removeWorkflowsPriorToCutoff();
		if(this.processOrphanedTaks) {
			removeRemainingTasksPriorToCutoff();
		} else {
			System.out.println(PROCESS_ORPHANED_TASKS_PROPNAME + " is set to false so we will not do that");
		}
		closeDataSource();
	}

	private void removeWorkflowsPriorToCutoff() throws Exception {
		System.out.println("***************************************************");
		System.out.println("REMOVING WORKFLOWS");
		System.out.println("***************************************************");
		long start = System.currentTimeMillis();
		System.out.println("Getting records to process for all queries");
		long totalRecsForAllQueries = getTotalWorkflowRecs();
		System.out.println("Total records to process for all queries: " + totalRecsForAllQueries);
		List<String> workflowIds = null;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int totalRecsProcessed = 0;
		int workflowsOnlyRemoved = 0, workflowsTerminatedAndRemoved = 0, workflowsRemovedManually = 0, workflowsNothingDone = 0;
		int offset = this.rowOffset;
		while(true) {
			int workflowsRemovedThisQuery = 0;
			int recsProcessed = 0;
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

				String query = "select workflow_id from workflow where modified_on < '" + this.cutoffDate + "'";
				if(this.rowLimit > 0) {
					query += (" order by modified_on limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("QUERY: " + query);

				rs = st.executeQuery(query);
				while(rs.next()) {
					workflowIds.add(rs.getString("workflow_id"));
				}
				if(workflowIds.size() == 0) {
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
			System.out.println("Found " + totalRecs + " workflow ids on this query");
			if(totalRecs > 0) {
				try {
					for(String workflowId : workflowIds) {
						recsProcessed++;
						totalRecsProcessed++;
						try {
							if(this.logEachRecord) {
								System.out.print("[" + new Date().toString() + "] Record #" + totalRecsProcessed + " of " + totalRecsForAllQueries + ", workflowId " + workflowId + "...");
							}
							WorkflowProcessResult result = processWorkflow(workflowId);
							if(result.removedOnly) {
								workflowsOnlyRemoved++;
								workflowsRemovedThisQuery++;
							} else if(result.terminatedAndRemoved){
								workflowsTerminatedAndRemoved++;
								workflowsRemovedThisQuery++;
							} else if(result.removedManually){
								workflowsRemovedManually++;
								workflowsRemovedThisQuery++;
							} else {
								workflowsNothingDone++;
							}
								
							if(this.logEachRecord) {
								if(result.removedOnly) {
									System.out.println("workflow " + result.workflowType + " removed only");
								} else if(result.terminatedAndRemoved) {
									System.out.println("workflow " + result.workflowType + " terminated then removed");
								} else if(result.removedManually) {
									System.out.println("workflow " + result.workflowType + " removed manually");
								} else { 							
									System.out.println("nothing done because of this error: " + result.errorMessage);
								}
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(totalRecsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " for this query out of " + totalRecs + ", " + 
									totalRecsProcessed + " processed for all queries out of " + totalRecsForAllQueries +
									" - workflows terminated and removed: " + workflowsTerminatedAndRemoved +
									" just removed: " + workflowsOnlyRemoved + 
									" removedManually: " + workflowsRemovedManually + 
									" workflows nothing done: " + workflowsNothingDone;
							System.out.println(msg);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(runningWorkflows.size() > 0) {
				System.out.println("************************************************************************************");
				System.out.println(TERMINATE_IF_RUNNING_PROPNAME + " was set to false. These are the first 1000 RUNNING workflows");
				runningWorkflows.forEach(id -> {
					System.out.println(id);
				});
				System.out.println("************************************************************************************");
				System.out.println("************************************************************************************");
				System.out.println(TERMINATE_IF_RUNNING_PROPNAME + " was set to false. These are the number of workflows by type not terminated");
				workflowsRunningNotTerminated.keySet().forEach(type -> {
					System.out.println(type + ": " + workflowsRunningNotTerminated.get(type));
					if(BUS_EXCEPTION_WORKFLOW.equals(type)) {
						this.busExceptionInventoryTypes.keySet().forEach(invtype -> {
							InventoryTypeInfo iti = this.busExceptionInventoryTypes.get(invtype);
							System.out.println("    " + invtype + ": count(" + iti.count + "), latest mod (" + iti.latestModTimestamp + ")");
						});
					}
				});
				System.out.println("************************************************************************************");
			}
			String msg = "[" + new Date().toString() + "] Processed " + totalRecsProcessed + " workflows, " +
					" workflows terminated and removed: " + workflowsTerminatedAndRemoved +
					" just removed: " + workflowsOnlyRemoved;
			String message = Utils.getProperty("which.app") + ": completed processing " + totalRecsProcessed + " workflows";
			System.out.println(msg);
			System.out.println(message);
			if(totalRecsProcessed >= totalRecsForAllQueries) {
				break;
			} else {
				if(this.rowLimit > 0) {
					offset += this.rowLimit;
					offset -= workflowsRemovedThisQuery;
				} else {
					break;
				}
			}
		}
		new EmailSender().sendRemoveOldWorkflowsResultsEmail(totalRecsProcessed, workflowsTerminatedAndRemoved, 
			workflowsOnlyRemoved, workflowsRemovedManually, workflowsNothingDone, System.currentTimeMillis() - start);
	}

	private void removeRemainingTasksPriorToCutoff() throws Exception {
		System.out.println("**************************************************************");
		System.out.println("REMOVING REMAINING TASKS NOT REMOVED BY REMOVING THE WORKFLOWS");
		System.out.println("***************************************************************");
		long start = System.currentTimeMillis();
		System.out.println("Getting records to process for all queries");
		long totalRecsForAllQueries = getTotalTaskRecs();
		System.out.println("Total records to process for all queries: " + totalRecsForAllQueries);
		List<String> taskIds = null;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int totalRecsProcessed = 0;
		int tasksRemoved = 0, workflowsExist = 0, nothingDone = 0;
		int offset = this.rowOffset;
		while(true) {
			int recsProcessed = 0;
			int tasksRemovedThisQuery = 0;
			taskIds = new ArrayList<String>();
			try {
				con = getDatabaseConnection();
				if(con == null) {
					throw new Exception("Could not get database connection");
				}
				con.setAutoCommit(false);
				st = con.createStatement();
		
				// Turn use of the cursor on.
				st.setFetchSize(100);

				String query = "select task_id from task where modified_on < '" + this.cutoffDate + "'";
				if(this.rowLimit > 0) {
					query += (" order by modified_on limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("QUERY: " + query);

				rs = st.executeQuery(query);
				while(rs.next()) {
					taskIds.add(rs.getString("task_id"));
				}
				if(taskIds.size() == 0) {
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
			int totalRecs = taskIds.size();
			System.out.println("Found " + totalRecs + " task ids on this query");
			if(totalRecs > 0) {
				try {
					for(String taskId : taskIds) {
						recsProcessed++;
						totalRecsProcessed++;
						try {
							if(this.logEachRecord) {
								System.out.println("[" + new Date().toString() + "] Record #" + totalRecsProcessed + " of " + totalRecsForAllQueries + ", taskId " + taskId);
							}
							TaskProcessResult result = processRemainingTask(taskId);
							if(result.removed) {
								tasksRemoved++;
								tasksRemovedThisQuery++;
							} else if(result.workflowExists){
								workflowsExist++;
							} else {
								nothingDone++;
							}
								
							if(this.logEachRecord) {
								if(result.removed) {
									System.out.println("  task removed");
								} else if(result.workflowExists) {
									System.out.println("  workflow exists, cannot remove task");
								} else { 							
									System.out.println("  nothing done because of this error: " + result.errorMessage);
								}
							}
						} catch(Exception e) {
							e.printStackTrace();
						}
						if(totalRecsProcessed % 1000 == 0) {
							String msg = "[" + new Date().toString() + "] Processed " + recsProcessed + " for this query out of " + totalRecs + ", " + 
									totalRecsProcessed + " processed for all queries out of " + totalRecsForAllQueries +
									" - tasks removed: " + tasksRemoved +
									" workflows exist: " + workflowsExist + 
									" nothing done: " + nothingDone;
							System.out.println(msg);
						}
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			String msg = "[" + new Date().toString() + "] Processed " + totalRecsProcessed + " tasks, " +
					" tasks removed: " + tasksRemoved +
					" workflows exist: " + workflowsExist;
			String message = Utils.getProperty("which.app") + ": completed processing " + totalRecsProcessed + " tasks";
			System.out.println(msg);
			System.out.println(message);
			if(totalRecsProcessed >= totalRecsForAllQueries) {
				break;
			} else {
				if(this.rowLimit > 0) {
					offset += this.rowLimit;
					offset -= tasksRemovedThisQuery;
				} else {
					break;
				}
			}
		}
		new EmailSender().sendRemainingTasksResultsEmail(totalRecsProcessed, tasksRemoved, workflowsExist, nothingDone,
			System.currentTimeMillis() - start);
	}

	@SuppressWarnings("deprecation")
	private WorkflowProcessResult processWorkflow(String workflowId) {
		WorkflowProcessResult result = new WorkflowProcessResult();
		String json = null;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		JsonElement workflowJsonElement = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT modified_on, json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, workflowId);
			rs = st.executeQuery();
			if(rs.next()) {
				json = rs.getString("json_data");
				try {
					workflowJsonElement = this.jsonParser.parse(json);
					JsonObject jo = workflowJsonElement.getAsJsonObject();
					JsonElement je = jo.get("workflowType");
					if(je != null) {
						result.workflowType = je.getAsString();
					} else {
						result.workflowType = getWorkflowType(json); 
					}
					if(BUS_EXCEPTION_WORKFLOW.equals(result.workflowType)) {
						je = jo.get("input");
						if(je != null) {
							jo = je.getAsJsonObject();
							if(jo != null) {
								je = jo.get("inventory");
								if(je != null) {
									jo = je.getAsJsonObject();
									if(jo != null) {
										je = jo.get("invType");
										if(je != null) {
											InventoryTypeInfo iti = this.busExceptionInventoryTypes.get(je.getAsString());
											if(iti == null) {
												iti = new InventoryTypeInfo();
												iti.count = new Integer(0);
											}
											iti.count = iti.count.intValue() + 1;
											iti.inventoryType = je.getAsString();
											Timestamp ts = rs.getTimestamp("modified_on");
											if(iti.latestModTimestamp == null || ts.getTime() > iti.latestModTimestamp.getTime()) {
												iti.latestModTimestamp = ts;
											}
											this.busExceptionInventoryTypes.put(iti.inventoryType, iti);
										}
									}
								}
							}
						}
					}
				} catch(Exception e) {
					if(this.logEachRecord) {
						System.out.print("error parsing workflow, assuming RUNNING: " + e.getMessage() + "...");
					}
					result.workflowType = getWorkflowType(json);
				}
			} else {
				result.errorMessage = "No workflow found for workflow id " + workflowId; 
				throw new Exception(result.errorMessage);
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

		if(workflowJsonElement == null || result.workflowType == null) {
			removeBadWorkflow(workflowId, result.workflowType, result);
		} else {
			removeWorkflow(workflowId, workflowJsonElement, result.workflowType, result);
		}

		return result;
	}

	private String getWorkflowType(String workflowJson) {
		return getAttributeValueFromJson(workflowJson, "workflowType");
	}
	
	private String getWorkflowInstanceId(String taskJson) {
		return getAttributeValueFromJson(taskJson, "workflowInstanceId");
	}
	
	private String getTaskDefName(String taskJson) {
		return getAttributeValueFromJson(taskJson, "taskDefName");
	}
	
	private String getReferenceTaskName(String taskJson) {
		return getAttributeValueFromJson(taskJson, "referenceTaskName");
	}

	private String getAttributeValueFromJson(String json, String attrName) {
		String value = null;
		String escaped = "\"" + attrName + "\":";
		int startingLoc = json.indexOf(escaped);
		int endingLoc = -1;
		if(startingLoc != -1) {
			startingLoc += escaped.length();
			endingLoc = json.indexOf(",", startingLoc);
			if(endingLoc != -1) {
				startingLoc++;
				endingLoc--;
				value = json.substring(startingLoc, endingLoc);
			}
		}
		return value;
	}

	@SuppressWarnings("deprecation")
	private void removeWorkflow(String workflowId, JsonElement workflowJson, String workflowType, WorkflowProcessResult result) {
		JsonObject jo = workflowJson.getAsJsonObject();
		JsonElement je = jo.get("status");
		if(je != null) {
			switch(je.getAsString()) {
			case "RUNNING":
			case "PAUSED":
				if(this.terminateIfRunning && (this.terminateAllWorkflowsIfRunning || this.workflowsToTerminateIfRunning.contains(result.workflowType))) {
					if(!this.logEachRecord && this.logOnlyPurged) {
						System.out.println("[" + new Date().toString() + "] About to terminate and remove workflow " + workflowId);
					}
					try {
						terminateWorkflow(workflowId, "Terminated-RunningTooLong");
					} catch(Exception e) {
						if(this.logEachRecord) {
							System.out.print("error terminating but continuing to remove...");
						}
					}
					try {
						// Sleep to give the terminate async time to actually terminate
						Thread.sleep(2000);
						removeWorkflow(workflowId);
						result.terminatedAndRemoved = true;
					} catch(Exception e) {
						e.printStackTrace();
						result.removedManually = true;
						removeManually(workflowId, workflowType);
					}
				} else {
					result.errorMessage = "workflow " + result.workflowType + " is running but this type of workflow is not supposed to be terminated";
					if(runningWorkflows.size() < MAX_RUNNING_WORKFLOWS) {
						runningWorkflows.add(workflowId);
					}
					Integer i = workflowsRunningNotTerminated.get(result.workflowType);
					if(i == null) {
						i = new Integer(0);
					}
					i = i.intValue() + 1;
					workflowsRunningNotTerminated.put(result.workflowType, i);
				}
				break;
			default:
				try {
					if(!this.logEachRecord && this.logOnlyPurged) {
						System.out.println("[" + new Date().toString() + "] About to remove workflow " + workflowId);
					}
					removeWorkflow(workflowId);
					result.removedOnly = true;
				} catch(Exception e) {
					e.printStackTrace();
					result.removedManually = true;
					removeManually(workflowId, workflowType);
				}
				break;
			}
		} else {
			if(this.logEachRecord) {
				System.out.print("status not found, assuming RUNNING...");
			}
			removeBadWorkflow(workflowId, workflowType, result);
		}
	}

	private void removeBadWorkflow(String workflowId, String workflowType, WorkflowProcessResult result) {
		try {
			terminateWorkflow(workflowId);
			if(this.logEachRecord) {
				System.out.print("terminated...");
			}
		} catch(Exception e) {
			if(this.logEachRecord) {
				System.out.print("error terminating: " + e.getMessage() + "...");
			}
			e.printStackTrace();
		}
		try {
			removeWorkflow(workflowId);
			if(this.logEachRecord) {
				System.out.print("removed...");
			}
			result.terminatedAndRemoved = true;
		} catch(Exception e) {
			if(this.logEachRecord) {
				System.out.print("error removing: " + e.getMessage() + "...");
			}
			e.printStackTrace();
			result.removedManually = true;
			removeManually(workflowId, workflowType);
		}
	}
	
	private void removeManually(String workflowId, String workflowType) {
		if(this.logEachRecord) {
			System.out.println("removing workflow rows from database manually...");
		}
		removeTasks(workflowId);
		removeWorkflowRows(workflowId, workflowType);
	}
	
	private void removeTasks(String workflowId) {
		List<String> taskIds = new ArrayList<String>();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			String query = "SELECT task_id FROM workflow_to_task where workflow_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, workflowId);
			rs = st.executeQuery();
			while(rs.next()) {
				taskIds.add(rs.getString("task_id"));
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
		if(taskIds.size() > 0) {
			removeTaskRows(workflowId, taskIds);
		} else {
			if(this.logEachRecord) {
				System.out.println("No tasks found for this workflow");
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	private TaskProcessResult processRemainingTask(String taskId) {
		TaskProcessResult result = new TaskProcessResult();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(true);
			String taskDefName = null, referenceTaskName = null, workflowId = null;
			boolean workflowExists = false;
			String json = null;
			JsonObject jo = null;
			JsonElement je = null;
			String query = "SELECT json_data FROM task where task_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, taskId);
			rs = st.executeQuery();
			if(rs.next()) {
				json = rs.getString("json_data");
				try {
					je = this.jsonParser.parse(json);
					jo = je.getAsJsonObject();
					je = jo.get("taskDefName");
					if(je != null) {
						taskDefName = je.getAsString();
					}
					je = jo.get("referenceTaskName");
					if(je != null) {
						referenceTaskName = je.getAsString();
					}
					je = jo.get("workflowInstanceId");
					if(je != null) {
						workflowId = je.getAsString();
					}
				} catch(Exception e) {
					workflowId = getWorkflowInstanceId(json);
					taskDefName = getTaskDefName(json);
					referenceTaskName = getReferenceTaskName(json);
				}
			} else {
				throw new Exception("**************** TASK NOT FOUND!!!!");
			}
			rs.close();
			st.close();
			
			query = "SELECT json_data FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, workflowId);
			rs = st.executeQuery();
			workflowExists = rs.next();
			st.close();

			if(workflowExists) {
				result.workflowExists = true;
			} else {
				result.removed = true;
				st = removeTaskFromTaskTables(con, workflowId, taskId, taskDefName, referenceTaskName);
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

	@SuppressWarnings("deprecation")
	private void removeTaskRows(String workflowId, List<String> taskIds) {
		if(this.logEachRecord) {
			System.out.println("  Removing " + taskIds.size() + " tasks for workflow " + workflowId + " from the database");
		}
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(true);
			String taskDefName = null, referenceTaskName = null;
			String json = null;
			JsonObject jo = null;
			JsonElement je = null;
			for(String taskId : taskIds) {
				if(this.logEachRecord) {
					System.out.println("    For task " + taskId + "...");
				}
				String query = "SELECT json_data FROM task where task_id = ?";
				st = con.prepareStatement(query);
				st.setString(1, taskId);
				rs = st.executeQuery();
				if(rs.next()) {
					json = rs.getString("json_data");
					try {
						je = this.jsonParser.parse(json);
						jo = je.getAsJsonObject();
						je = jo.get("taskDefName");
						if(je != null) {
							taskDefName = je.getAsString();
						}
						je = jo.get("referenceTaskName");
						if(je != null) {
							referenceTaskName = je.getAsString();
						}
					} catch(Exception e) {
						taskDefName = getTaskDefName(json);
						referenceTaskName = getReferenceTaskName(json);
					}
				}
				st.close();
				st = removeTaskFromTaskTables(con, workflowId, taskId, taskDefName, referenceTaskName);
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
	
	private PreparedStatement removeTaskFromTaskTables(Connection con, String workflowId, String taskId, String taskDefName, String referenceTaskName) throws Exception {
		PreparedStatement st = null;
		if(taskDefName == null) {
			System.out.println("  ************* COULD NOT FIND taskDefName!!!!!!!!!!");
		}
		if(referenceTaskName == null) {
			System.out.println("  ************* COULD NOT FIND referenceTaskName!!!!!!!!!!");
		}
		String query = "DELETE FROM workflow_to_task where workflow_id = ? and task_id = ?";
		st = con.prepareStatement(query);
		if(this.logEachRecord) {
			System.out.print("      Deleting rows from workflow_to_task table...");
		}
		st.setString(1, workflowId);
		st.setString(2, taskId);
		int rowsDeleted = st.executeUpdate();
		if(this.logEachRecord) {
			System.out.println("deleted " + rowsDeleted + " rows");
		}
		st.close();

		query = "DELETE FROM task_in_progress where task_id = ?";
		if(taskDefName != null) {
			query += " and task_def_name = ?";
		}
		st = con.prepareStatement(query);
		if(this.logEachRecord) {
			System.out.print("      Deleting rows from task_in_progress table...");
		}
		st.setString(1, taskId);
		if(taskDefName != null) {
			st.setString(2, taskDefName);
		}
		rowsDeleted = st.executeUpdate();
		if(this.logEachRecord) {
			System.out.println("deleted " + rowsDeleted + " rows");
		}
		st.close();

		query = "DELETE FROM task_scheduled where workflow_id = ?";
		if(referenceTaskName != null) {
			query += " and task_key like ?";
		}
		st = con.prepareStatement(query);
		if(this.logEachRecord) {
			System.out.print("      Deleting rows from task_scheduled table...");
		}
		st.setString(1, workflowId);
		if(referenceTaskName != null) {
			st.setString(2, referenceTaskName + "%");
		}
		rowsDeleted = st.executeUpdate();
		if(this.logEachRecord) {
			System.out.println("deleted " + rowsDeleted + " rows");
		}
		st.close();
		
		query = "DELETE FROM task where task_id = ?";
		st = con.prepareStatement(query);
		if(this.logEachRecord) {
			System.out.print("      Deleting task from task table...");
		}
		st.setString(1, taskId);
		rowsDeleted = st.executeUpdate();
		if(this.logEachRecord) {
			System.out.println("deleted " + rowsDeleted + " rows");
		}
		
		return st;
	}

	private void removeWorkflowRows(String workflowId, String workflowType) {
		if(this.logEachRecord) {
			System.out.println("  Removing workflow rows for workflow " + workflowId + " from the database");
		}
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(true);
			String query = "DELETE FROM workflow_pending where workflow_id = ?";
			if(workflowType != null) {
				query += " and workflow_type = ?";
			}
			st = con.prepareStatement(query);
			if(this.logEachRecord) {
				System.out.print("      Deleting rows from workflow_pending table...");
			}
			st.setString(1, workflowId);
			if(workflowType != null) {
				st.setString(2, workflowType);
			}
			int rowsDeleted = st.executeUpdate();
			if(this.logEachRecord) {
				System.out.println("deleted " + rowsDeleted + " rows");
			}
			st.close();

			query = "DELETE FROM workflow_def_to_workflow where workflow_id = ?";
			if(workflowType != null) {
				query += " and workflow_def = ?";
			}
			st = con.prepareStatement(query);
			if(this.logEachRecord) {
				System.out.print("      Deleting rows from workflow_def_to_workflow table...");
			}
			st.setString(1, workflowId);
			if(workflowType != null) {
				st.setString(2, workflowType);
			}
			rowsDeleted = st.executeUpdate();
			if(this.logEachRecord) {
				System.out.println("deleted " + rowsDeleted + " rows");
			}
			st.close();

			query = "DELETE FROM workflow where workflow_id = ?";
			st = con.prepareStatement(query);
			if(this.logEachRecord) {
				System.out.print("      Deleting rows from workflow table...");
			}
			st.setString(1, workflowId);
			rowsDeleted = st.executeUpdate();
			if(this.logEachRecord) {
				System.out.println("deleted " + rowsDeleted + " rows");
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
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
	
	private long getTotalWorkflowRecs() throws Exception {
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
	
			String query = "SELECT count(*) as total FROM workflow where modified_on < '" + this.cutoffDate + "'";
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
	
	private long getTotalTaskRecs() throws Exception {
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
	
			String query = "SELECT count(*) as total FROM task where modified_on < '" + this.cutoffDate + "'";
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
	
	private class WorkflowProcessResult {
		String workflowType = null;
		String errorMessage = null;
		boolean removedOnly = false;
		boolean terminatedAndRemoved = false;
		boolean removedManually = false;
	}
	
	private class InventoryTypeInfo {
		String inventoryType = null;
		Integer count = null;
		Timestamp latestModTimestamp = null;
	}
	private class TaskProcessResult {
		String errorMessage = null;
		boolean removed = false;
		boolean workflowExists = false;
	}
}
