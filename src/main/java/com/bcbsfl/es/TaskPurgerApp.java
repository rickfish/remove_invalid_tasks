package com.bcbsfl.es;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class TaskPurgerApp {
    private static final String CONDUCTOR_URL_PROPNAME = "conductor.url";
    private static final String CONDUCTOR_USERID_PROPNAME = "conductor.userid";
    private static final String CONDUCTOR_PASSWORD_PROPNAME = "conductor.password";
    private static final String ACTUALLY_PURGE_PROPNAME = "actually.purge";
    private static final String LOG_ONLY_PURGED_PROPNAME = "log.only.purged";
    private static final String ROW_LIMIT_PROPNAME = "row.limit";
    private static final String ROW_OFFSET_PROPNAME = "row.offset";
    private static final String SAMPLE_ID_COUNT_PROPNAME = "sample.id.count";
    static protected final Charset CHARACTER_SET = Charset.forName("iso-8859-1");
    protected String env = null;
    private String conductorUrl;
    private String authString = null;
    protected boolean logEachRecord = false;
    protected boolean logOnlyPurged = false;
    protected String outputDirectory = null;
    protected boolean actuallyPurge = false;
    protected int rowLimit = 0;
    protected int rowOffset = 0;
    protected int sampleIdCount = 0;
    private Long dsLock = new Long(0);
	private static HikariDataSource ds = null;
   
	private JsonParser jsonParser = new JsonParser();

	public TaskPurgerApp() {
    	init();
    }
    
	protected void init() {
		try {
            this.env = Utils.getProperty("env");
	    	conductorUrl = Utils.getProperty(CONDUCTOR_URL_PROPNAME);
	    	if(conductorUrl == null) {
	    		throw new Exception("No 'conductor.url' property was specified");
	    	}
    		System.out.println("Using " + conductorUrl + " as the conductor url");
	    	String userId = Utils.getProperty(CONDUCTOR_USERID_PROPNAME);
	    	if(userId == null) {
	    		System.out.println("No " + CONDUCTOR_USERID_PROPNAME + " was specified so we won't be using an auth header on the 'decide' call");
	    	} else {
	    		String password = Utils.getProperty(CONDUCTOR_PASSWORD_PROPNAME);
	           	this.authString = getBasicAuthHeaderValue(userId, password.getBytes(CHARACTER_SET));
	    	}
            this.outputDirectory = Utils.getProperty("output.directory");
			this.logEachRecord = Utils.getBooleanProperty("log.each.record");
	    	this.logOnlyPurged = Utils.getBooleanProperty(LOG_ONLY_PURGED_PROPNAME);
	    	this.actuallyPurge = Utils.getBooleanProperty(ACTUALLY_PURGE_PROPNAME);
	    	this.rowLimit = Utils.getIntProperty(ROW_LIMIT_PROPNAME);
	    	this.rowOffset = Utils.getIntProperty(ROW_OFFSET_PROPNAME);
	    	this.sampleIdCount = Utils.getIntProperty(SAMPLE_ID_COUNT_PROPNAME);
    		System.out.println("The " + ACTUALLY_PURGE_PROPNAME + " property says to " + (this.actuallyPurge ? "" : "NOT") + " actually purge the messages");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void createDataSource() {
		try {
			synchronized(dsLock) {
				if(ds == null) {
			    	HikariConfig config = new HikariConfig();
					config.setJdbcUrl(Utils.getProperty("db.url"));
					config.setUsername(Utils.getProperty("db.user"));
					config.setPassword(Utils.getProperty("db.password"));
					config.setMaximumPoolSize(5);
					config.setIdleTimeout(30000);
					config.setMinimumIdle(1);
		            config.setAutoCommit(false);
		            config.addDataSourceProperty("cachePrepStmts", "true");
		            config.addDataSourceProperty("prepStmtCacheSize", "250");
		            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");		
		            ds = new HikariDataSource(config);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void closeDataSource() {
/* Don't close it because creating it again will cause connections to be created again and eventually we run out of connections		
		try {
            ds.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
*/		
	}
	
	protected void done() {
	}
	
    protected String getBasicAuthHeaderValue(final String username, final byte[] password) {
        try {

            final byte[] prefix = (username + ":").getBytes(CHARACTER_SET);
            final byte[] usernamePassword = new byte[prefix.length + password.length];

            System.arraycopy(prefix, 0, usernamePassword, 0, prefix.length);
            System.arraycopy(password, 0, usernamePassword, prefix.length, password.length);

            return "Basic " + new String(Base64.getEncoder().encode(usernamePassword), "ASCII");
        } catch (UnsupportedEncodingException ex) {
            // This should never occur
            throw new RuntimeException(ex);
        }
    }
    
    protected Connection getDatabaseConnection() {
    	Connection con = null;
		try {
			con = ds.getConnection();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return con;
    }
    
    protected void terminateWorkflow(String workflowId) throws Exception {
    	terminateWorkflow(workflowId, "TerminatedByRemoveOldWorkflowsApp");
    }

    protected void terminateWorkflow(String workflowId, String reason) throws Exception {
    	if(this.actuallyPurge) {
	    	String urlString = this.conductorUrl + "/conductor/api/v1/workflow/" + workflowId + "?reason=" + reason;
	    	HttpURLConnection conn = (HttpURLConnection) new URL(urlString).openConnection();
	    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
	    	if(this.authString != null) {
	    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
	    	}
	   		conn.setRequestMethod("DELETE");
	    	conn.setRequestProperty("charset", "utf-8");
	    	conn.setDoOutput(true);
	    	conn.connect();
	    	int code = conn.getResponseCode();
	    	if(code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
	    		String message = "Got a response code of " + code + " (" + conn.getResponseMessage() + ") while trying to call the 'DELETE workflow' endpoint for workflow id '" + workflowId + "'";
	        	System.out.println(message);
				throw new IOException(message);
	    	}
    	}    	
    }

    protected void removeWorkflow(String workflowId) throws Exception {
    	if(this.actuallyPurge) {
	    	String urlString = this.conductorUrl + "/conductor/api/v1/workflow/" + workflowId + "/remove?archiveWorkflow=true";
	    	HttpURLConnection conn = (HttpURLConnection) new URL(urlString).openConnection();
	    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
	    	if(this.authString != null) {
	    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
	    	}
	   		conn.setRequestMethod("DELETE");
	    	conn.setRequestProperty("charset", "utf-8");
	    	conn.setDoOutput(true);
	    	conn.connect();
	    	int code = conn.getResponseCode();
	    	if(code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
	    		String message = "Got a response code of " + code + " (" + conn.getResponseMessage() + ") while trying to call the 'DELETE workflow/remove' endpoint for workflow id '" + workflowId + "'";
	        	System.out.println(message);
				throw new IOException(message);
	    	}
    	}    	
    }

    protected void terminateTask(String taskId, String status) throws Exception {
    	if(this.actuallyPurge) {
	    	String urlString = this.conductorUrl + "/conductor/api/v1/tasks/" + taskId;
	    	HttpURLConnection conn = (HttpURLConnection) new URL(urlString).openConnection();
	    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
	    	if(this.authString != null) {
	    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
	    	}
	   		conn.setRequestMethod("GET");
	    	conn.setRequestProperty("charset", "utf-8");
	    	conn.setDoOutput(true);
	    	conn.connect();
	    	int code = conn.getResponseCode();
	    	if(code == HttpStatus.SC_OK) {
	    		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
	    		StringBuilder sb = new StringBuilder();
	    		String output;
	    		while ((output = br.readLine()) != null) {
	    		  sb.append(output);
	    		}
	    		conn.getInputStream().close();
	    		String response = sb.toString();
	    		JsonElement je = this.jsonParser.parse(response);
	    		JsonObject jo = je.getAsJsonObject();
	    		jo.addProperty("status", status);
		    	urlString = this.conductorUrl + "/conductor/api/v1/tasks";
		    	conn = (HttpURLConnection) new URL(urlString).openConnection();
		    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
		    	if(this.authString != null) {
		    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
		    	}
		   		conn.setRequestMethod("POST");
		    	conn.setRequestProperty("charset", "utf-8");
		    	conn.setDoOutput(true);
		    	try(OutputStream os = conn.getOutputStream()) {
		    	    byte[] input = jo.toString().getBytes("utf-8");
		    	    os.write(input, 0, input.length);			
		    	}		    	
		    	conn.connect();
		    	code = conn.getResponseCode();
		    	if(code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
		    		String message = "Got a response code of " + code + " while trying to call the 'POST Task' endpoint for task id '" + taskId + "'";
		        	System.out.println(message);
					throw new IOException(message);
		    	}
	    	} else {
	    		String message = "Got a response code of " + code + " while trying to call the 'GET Task' endpoint for task id '" + taskId + "'";
	        	System.out.println(message);
				throw new IOException(message);
	    	}
    	}    	
    }

    protected void makeDecideCall(String workflowId) throws Exception {
    	if(this.actuallyPurge) {
	    	String urlString = this.conductorUrl + "/conductor/api/v1/workflow/decide/" + workflowId;
	    	HttpURLConnection conn = (HttpURLConnection) new URL(urlString).openConnection();
	    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
	    	if(this.authString != null) {
	    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
	    	}
	   		conn.setRequestMethod("PUT");
	    	conn.setRequestProperty("charset", "utf-8");
	    	conn.setDoOutput(true);
	    	conn.connect();
	    	int code = conn.getResponseCode();
	    	if(code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
	    		String message = "Got a response code of " + code + " while trying to call the 'decide' endpoint for workflow id '" + workflowId + "'";
	        	System.out.println(message);
				throw new IOException(message);
	    	}
    	}    	
    }

    protected void runTaskSearch(int start, int size, String sort, String query) throws Exception {
    	boolean useAmpersand = false;
    	String urlString = this.conductorUrl + "/conductor/api/v1/tasks/search";
    	if(start > 0 ) {
    		urlString += (useAmpersand ? "&" : "?");
    		urlString += "start=" + start;
    		useAmpersand = true;
    	}
    	if(size > 0 ) {
    		urlString += (useAmpersand ? "&" : "?");
    		urlString += "size=" + size;
    		useAmpersand = true;
    	}
    	if(StringUtils.isNotBlank(sort)) {
    		urlString += (useAmpersand ? "&" : "?");
    		urlString += "sort=" + sort;
    		useAmpersand = true;
    	}
    	if(StringUtils.isNotBlank(query)) {
    		urlString += (useAmpersand ? "&" : "?");
    		urlString += "freeText=" + query;
    		useAmpersand = true;
    	}
    	System.out.println("URL: " + urlString);
    	long startTime = System.currentTimeMillis();
    	HttpURLConnection conn = (HttpURLConnection) new URL(urlString).openConnection();
    	conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
    	if(this.authString != null) {
    		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, this.authString);
    	}
   		conn.setRequestMethod("GET");
    	conn.setRequestProperty("charset", "utf-8");
    	conn.setDoOutput(true);
    	conn.connect();
    	int code = conn.getResponseCode();
    	System.out.println("Tasks search took " + (System.currentTimeMillis() - startTime) + " ms");
    	if(code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
    		String message = "Got a response code of " + code + " while trying to call the 'tasks search' endpoint for start " + start + " size " + size + " sort " + sort + " query " + query;
        	System.out.println(message);
			throw new IOException(message);
    	}
    }

    protected void unack(Connection con, QueueResult qr, QueueMessage qm) {
    	if(!this.actuallyPurge) {
    		return;
    	}
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			String query = "SELECT message_id from queue_message where popped = true and queue_name = ? and message_id = ? FOR UPDATE SKIP LOCKED";
			st = con.prepareStatement(query);
			st.setString(1, qm.queue);
			st.setString(2, qm.id);
			qr.notPopped = true;
			rs = st.executeQuery();
			if(rs.next()) {
				st.close();
				query = "UPDATE queue_message set popped = false where queue_name = ? and message_id = ?";
				st = con.prepareStatement(query);
				st.setString(1, qm.queue);
				st.setString(2, qm.id);
				int rowsUpdated = st.executeUpdate();
				if(rowsUpdated > 0) {
					qr.notPopped = false;
					qr.unacked = true;
				}
			} else {
				st.close();
				query = "UPDATE queue_message set popped = true where queue_name = ? and message_id = ?";
				st = con.prepareStatement(query);
				st.setString(1, qm.queue);
				st.setString(2, qm.id);
				st.executeUpdate();
			}
			con.commit();
		} catch(Exception e) {
			try {
				con.rollback();
				e.printStackTrace();
			} catch(Exception e2) {
				e2.printStackTrace();
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
		}
    }

    protected void deleteQueueMessage(Connection con, QueueResult qr, QueueMessage qm) {
    	if(!this.actuallyPurge) {
    		return;
    	}
		PreparedStatement st = null;
		try {
			con.setAutoCommit(true);
			String query = null;
			if(!qr.isWorkflow && qr.workflowInstanceId != null && qr.taskReferenceName != null) {
				query = "DELETE FROM task_scheduled where workflow_id = ? and task_key like ?";
				st = con.prepareStatement(query);
				st.setString(1, qr.workflowInstanceId);
				st.setString(2, qr.taskReferenceName + "%");
				int rowsDeleted = st.executeUpdate();
				if(this.logEachRecord) {
					System.out.print("Deleted " + rowsDeleted + " from task_scheduled...");
				}
				st.close();
			} else if(qr.isWorkflow && qr.queue != null) {
				query = "DELETE FROM workflow_pending where workflow_id = ? and workflow_type = ?";
				st = con.prepareStatement(query);
				st.setString(1, qm.id);
				st.setString(2, qr.queue);
				int rowsDeleted = st.executeUpdate();
				if(this.logEachRecord) {
					System.out.print("Deleted " + rowsDeleted + " from workflow_pending...");
				}
				st.close();
			}
			
			query = "DELETE from queue_message where queue_name = ? and message_id = ?";
			st = con.prepareStatement(query);
			st.setString(1, qm.queue);
			st.setString(2, qm.id);
			int rowsDeleted = st.executeUpdate();
			if(this.logEachRecord) {
				System.out.print("Deleted " + rowsDeleted + " from queue_message...");
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
    }

    protected String getTaskJson(String taskId) {
		String taskJson = null;
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
			st.setString(1, taskId);
			rs = st.executeQuery();
			if(rs.next()) {
				taskJson = rs.getString("json_data");
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
		return taskJson;
	}

	protected String getWorkflowJson(String workflowId) {
		String workflowJson = null;
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
			st.setString(1, workflowId);
			rs = st.executeQuery();
			if(rs.next()) {
				workflowJson = rs.getString("json_data");
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
		return workflowJson;
	}

	protected String convertMilliseconds(long milliseconds) {
		long hours = milliseconds / (60 * 60 * 1000);
		long remaining = milliseconds % (60 * 60 * 1000);
		long minutes = remaining / (60 * 1000);
		remaining = remaining % (60 * 1000);
		long seconds = remaining / 1000;
		return "" + hours + " hours, " + minutes + " minutes, " + seconds + " seconds";
    }
}