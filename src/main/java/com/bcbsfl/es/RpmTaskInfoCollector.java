package com.bcbsfl.es;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RpmTaskInfoCollector extends TaskPurgerApp {
	private JsonParser jsonParser = new JsonParser();
	
	public RpmTaskInfoCollector() {
	}

	public void collect() {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		int recsProcessed = 0;
		List<String> taskIds = new ArrayList<String>(); 
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

			String query = "select message_id from queue_message where queue_name = 'SSO_RPM_DELAY_TASK' limit 5000";
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
		System.out.println("Found " + taskIds.size() + " tasks");
		if(taskIds.size() > 0) {
			for(String taskId : taskIds) {
				recsProcessed++;
				try {
					reportOnTask(taskId);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		closeDataSource();
	}

	private void reportOnTask(String taskId) {
		String taskJson = getTaskJson(taskId);
		int pollCount = 0, retryCount = 0;
		if(taskJson != null) {
			JsonElement je = this.jsonParser.parse(taskJson);
			JsonObject jo = je.getAsJsonObject();
			je = jo.get("pollCount");
			if(je != null) {
				pollCount = je.getAsInt();
			}
			je = jo.get("retryCount");
			if(je != null) {
				retryCount = je.getAsInt();
			}
			System.out.println("Task " + taskId + " pollCount " + pollCount + " retryCount " + retryCount);
		}
	}
}