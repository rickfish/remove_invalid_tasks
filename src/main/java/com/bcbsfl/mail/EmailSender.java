package com.bcbsfl.mail;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcbsfl.es.QueueResults;
import com.bcbsfl.es.QueueStatistics;
import com.bcbsfl.es.Utils;

/**
 * Sends standard emails to Conductor interested parties
 */
public class EmailSender {
    private static final Logger logger = LoggerFactory.getLogger(EmailSender.class);
    /**
     * Maximum number of stacktrace lines printed for the first exception in the nested list of exceptions
     */
    private static final int MAX_FIRST_EXCEPTION_STACKELEMENTS = 30;
    /**
     * Maximum number of stacktrace lines printed for the 'caused by' exceptions
     */
    private static final int MAX_ADDL_EXCEPTION_STACKELEMENTS = 15;

    private Session mailSession = null;
	private String smtpHost = null;
	private String from = null;
	private String replyTo = null;
	private String to = null;
	private String environment = null;
	
	public EmailSender() {
		this.smtpHost = getTrimmedString(Utils.getProperty("email.smtp.host"));
		if(this.smtpHost != null) {
			this.from = getTrimmedString(Utils.getProperty("email.address.from"));
			this.replyTo = getTrimmedString(Utils.getProperty("email.address.replyto"));
			this.to = getTrimmedString(Utils.getProperty("email.address.to"));
			Properties props = System.getProperties();
			props.put("mail.smtp.host", this.smtpHost);
			this.mailSession = Session.getInstance(props, null);
			this.environment = Utils.getProperty("env");
		}
	}
	
	/**
	 * Send an email that tells the recipient(s) that there was an exception in Conductor. For this email, we have the 
	 * error message to be displayed and the exception that was generated
	 * @param message a message to display in the email describing the exception context
	 * @param exception the exception
	 */
	public void sendExceptionEmail(String message, Throwable exception) {
		if(this.smtpHost == null) {
			return;
		}
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			/*
			 * Have the lambda function add the specific email message
			 */
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			if(exception != null) {
				sb.append("<div>Message: <span style='font-weight:bold;'>" + exception.getMessage() + "</span></div>");
				sb.append("<div style='text-decoration:underline;margin-top:5px;margin-bottom:2px;font-size:9px;font-weight:bold;font-family:Arial Black,Gadget,sans-serif'>STACK TRACE</div>");
				sb.append("<div style='font-size:10px;font-family:Courier New,Courier,monospace'>");
				addExceptionStackTrace(true, 1, sb, exception);
			} else {
				sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			}
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	public void sendResultsEmail(int recsProcessed, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "Reported on " + recsProcessed + " old SSO_RPM workflows";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(String appName, int recsProcessed, int recsTerminated, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = appName + " processed " + recsProcessed + "  tasks and " + recsTerminated + " were terminated";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Tasks processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Tasks terminated: <span style='font-weight:bold;'>" + recsTerminated + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(int recsProcessed, int recsSuccessful, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "Removed " + recsSuccessful + " invalid " + Utils.getProperty("task.type") + " tasks";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records with popped=false: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail("Removed invalid " + Utils.getProperty("task.type") + " tasks", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(boolean finisthed, int recsProcessed,  int workflowsTerminated, int medicalRecordsWorkflowsTerminated, 
			int careGapWorkflowsTerminated, int codingGapWorkflowsTerminated, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "Terminated " + workflowsTerminated + " old SSO_RPM workflows ";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Tasks processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Total workflows terminated: <span style='font-weight:bold;'>" + workflowsTerminated + "</span></div>");
			sb.append("<div>Medical Records workflows terminated: <span style='font-weight:bold;'>" + medicalRecordsWorkflowsTerminated + "</span></div>");
			sb.append("<div>Care Gap workflows terminated: <span style='font-weight:bold;'>" + careGapWorkflowsTerminated + "</span></div>");
			sb.append("<div>Coding Gap workflows terminated: <span style='font-weight:bold;'>" + codingGapWorkflowsTerminated + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(int recsProcessed, int limit, int offset, int recsPurged, int totalPurged, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "Removed " + recsPurged + " invalid messages";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed this run: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Messages purged this run: <span style='font-weight:bold;'>" + recsPurged + "</span></div>");
			if(limit > 0) {
				sb.append("<div>Started at resultset offset: <span style='font-weight:bold;'>" + offset + "</span></div>");
				sb.append("<div>Total messages purged: <span style='font-weight:bold;'>" + totalPurged + "</span></div>");
			}
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail("Removed " + recsPurged + " invalid messages from the queue_message table", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(String workflowName, int recsProcessed, int totalWaitTasksCompleted, int totalWorkflowsDecided, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "CdtClearWorkflows Processed " + recsProcessed + " " + workflowName + " workflows";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>WorkflowName: <span style='font-weight:bold;'>" + workflowName + "</span></div>");
			sb.append("<div>Workflows processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>WAIT tasks completed: <span style='font-weight:bold;'>" + totalWaitTasksCompleted + "</span></div>");
			sb.append("<div>Workflows decided: <span style='font-weight:bold;'>" + totalWorkflowsDecided + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	public void sendResultsEmail(int recsProcessed, int tasksTerminated, int delayTasksTerminated, int ackTasksTerminated, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String message = "Terminated " + tasksTerminated + " SSO_RPM tasks";
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Correlation ids processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("<div>Tasks terminated: <span style='font-weight:bold;'>" + tasksTerminated + "</span></div>");
			sb.append("<div>SSO_RPM_DELAY_TASK tasks terminated: <span style='font-weight:bold;'>" + delayTasksTerminated + "</span></div>");
			sb.append("<div>SSO_RPM_CHECK_HOME_PLAN_ACK_TASK tasks terminated: <span style='font-weight:bold;'>" + ackTasksTerminated + "</span></div>");
			sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	public void sendResultsEmail(int recsProcessed, 
			Map<String,	Map<String, QueueStatistics>> taskQueueStatistics, 
			Map<String,	Map<String, QueueStatistics>> workflowQueueStatistics, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed: <span style='font-weight:bold;'>" + recsProcessed + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail("Queue Statistics Collection", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email. Got this exception", e);
		}
	}

	public void sendResultsEmail(int totalRecsProcessed, QueueResults totalQueueResults, List<QueueResults> queueResults, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed: <span style='font-weight:bold;'>" + totalRecsProcessed + "</span></div>");
			sb.append("<div>Unacked queue messages: <span style='font-weight:bold;'>" + totalQueueResults.unacked + "</span></div>");
			sb.append("<div>RPM queue messages (not unacked): <span style='font-weight:bold;'>" + totalQueueResults.rpmObjects + "</span></div>");
			sb.append("<div>Queue messages deleted (not in progress): <span style='font-weight:bold;'>" + totalQueueResults.notnProgress + "</span></div>");
			sb.append("<div>Queue messages not popped when we got to them: <span style='font-weight:bold;'>" + totalQueueResults.notPopped + "</span></div>");
			sb.append("<div style='text-decoration:underline;margin-top:5px;margin-bottom:2px;font-size:9px;font-weight:bold;font-family:Arial Black,Gadget,sans-serif'>RECORDS PROCESSED BY RESOURCE</div>");
			sb.append("<table>");
			sb.append("<tr style='margin-top:5px;margin-bottom:2px;font-size:9px;font-weight:bold;font-family:Arial Black,Gadget,sans-serif;color:blue'><td style='margin-right:150px'>RESOURCE TYPE</td><td>RESOURCE NAME</td><td>UNACKED</td><td>RPM OBJECTS</td><td>DELETED</td><td>NOT POPPED</td></tr>");
			queueResults.forEach(qr -> {
				sb.append("<tr style='font-size:9px;font-weight:normal;font-family:Arial Black,Gadget,sans-serif;color:black'><td>" + (qr.isWorkflow ? "Workflow" : "Task") + "</td>");
				sb.append("<td>" + qr.name + "</td>");
				sb.append("<td>" + qr.unacked + "</td>");
				sb.append("<td>" + qr.rpmObjects + "</td>");
				sb.append("<td>" + qr.notnProgress + "</td>");
				sb.append("<td>" + qr.notPopped + "</td>");
				sb.append("</tr>");
			});
			sb.append("</table>");
			sb.append("</div></body></html>");
			sendEmail("Unacked queue messages", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email. Got this exception", e);
		}
	}
	
	public void sendRemoveOldWorkflowsResultsEmail(int totalRecsProcessed, int workflowsTerminatedAndRemoved, 
			int workflowsOnlyRemoved, int workflowsRemovedManually, int workflowsNothingDone, long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed: <span style='font-weight:bold;'>" + totalRecsProcessed + "</span></div>");
			sb.append("<div>Workflows terminated and removed: <span style='font-weight:bold;'>" + workflowsTerminatedAndRemoved + "</span></div>");
			sb.append("<div>Workflows removed only: <span style='font-weight:bold;'>" + workflowsOnlyRemoved + "</span></div>");
			sb.append("<div>Workflows removed manually: <span style='font-weight:bold;'>" + workflowsRemovedManually + "</span></div>");
			sb.append("<div>Workflows with nothing done: <span style='font-weight:bold;'>" + workflowsNothingDone + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail("Old workflows removed", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email. Got this exception", e);
		}
	}

	public void sendRemainingTasksResultsEmail(int totalRecsProcessed, int tasksRemoved, int workflowsExist, int nothingDone,
		long totalMilliseconds) {
		if(this.smtpHost == null) {
			return;
		}
		String timingMessage = getTimingMessage(totalMilliseconds);
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			sb.append("<div>Time to run: <span style='font-weight:bold;'>" + timingMessage + "</span></div>");
			sb.append("<div>Records processed: <span style='font-weight:bold;'>" + totalRecsProcessed + "</span></div>");
			sb.append("<div>Orphaned tasks removed: <span style='font-weight:bold;'>" + tasksRemoved + "</span></div>");
			sb.append("<div>Taks not orpnaned because their Workflow exists: <span style='font-weight:bold;'>" + workflowsExist + "</span></div>");
			sb.append("<div>Tasks with nothing done fpr other reasons: <span style='font-weight:bold;'>" + nothingDone + "</span></div>");
			sb.append("</div></body></html>");
			sendEmail("Orphaned tasks removed", sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email. Got this exception", e);
		}
	}
			
	private String getTimingMessage(long totalMilliseconds) {
		long hours = totalMilliseconds / (60 * 60 * 1000);
		long remaining = totalMilliseconds % (60 * 60 * 1000);
		long minutes = remaining / (60 * 1000);
		remaining = remaining % (60 * 1000);
		long seconds = remaining / 1000;
		return "" + hours + " hours, " + minutes + " minutes, " + seconds + " seconds";
	}
	
	/**
	 * Send an email in html format 
	 * @param body the body of the email
	 */
	public void sendEmail(String message, String body) {
		if(this.smtpHost == null) {
			return;
		}
		try {
			MimeMessage msg = getMessage();
			msg.setSubject("[" + this.environment + "] " + message, "UTF-8");
			msg.setText(body, "utf-8", "html");
			Transport.send(msg);
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	/**
	 * Print a stacktrace in the body of the email
	 * @param first whether or not this is the first email in a specified time period
	 * @param exceptionCount the number of exceptions of this type that occurred in the time period
	 * @param sb the StringBuffer containing the body of the email
	 * @param e the exception for which to print the stack trace
	 */
	private void addExceptionStackTrace(boolean first, int exceptionCount, StringBuffer sb, Throwable e) {
		if(e != null) {
			StackTraceElement steArray[] = e.getStackTrace();
			int elementsLeft = steArray.length;
			int elementNumber = 0;
			int maxElements = (first ? MAX_FIRST_EXCEPTION_STACKELEMENTS : MAX_ADDL_EXCEPTION_STACKELEMENTS);
			sb.append("<span style='font-weight: bold;color:red'>");
			if(!first) {
				sb.append("<span style='font-weight: bold;color:red'>Caused by: </span>");
			}
			sb.append("<span style='font-weight: bold;color:blue'>" + e.getClass().getName() + "</span>");
			if(e.getMessage() != null) {
				sb.append("<span style='font-weight: bold;color:red'>: " + e.getMessage() + "</span>");
			}
			sb.append("</br>");
			for(StackTraceElement ste: steArray) {
				elementNumber++;
				elementsLeft--;
				sb.append("<span style='color:red;font-weight:normal'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;at " + this.getFullStackTraceElementMethodName(ste) 
					+ "(</span><span style='color:blue;font-weight:normal'>" + ste.getFileName());
				if(ste.getLineNumber() > 0) {
					sb.append(":" + ste.getLineNumber());
				}
				sb.append("</span><span style='color:red;font-weight:normal'>)</span></br>");
				if(elementNumber > maxElements && elementsLeft > 0) {
					sb.append("<span style='color:red;font-weight:normal'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;... " + elementsLeft + " more</span></br>");
					break;
				}
			}
			if(exceptionCount < 5 && e.getCause() != null) {
				addExceptionStackTrace(false, exceptionCount + 1, sb, e.getCause());
			}
		}
	}
	
	private String getFullStackTraceElementMethodName(StackTraceElement ste) {
		String methodName = StringUtils.isNotBlank(ste.getMethodName()) ? ste.getMethodName().replace("<", "&lt;") : "";
		methodName = methodName.replace(">",  "&gt;");
		return ste.getClassName() + (StringUtils.isNotBlank(methodName) ? "." + methodName : "") ;
	}
	
	private MimeMessage getMessage() throws Exception {
		MimeMessage msg = new MimeMessage(this.mailSession);
		msg.addHeader("format", "flowed");
		msg.addHeader("Content-Transfer-Encoding", "8bit");
		if(StringUtils.isNotEmpty(this.from)) {
			msg.setFrom(new InternetAddress(this.from));
		}
		if(StringUtils.isNotEmpty(this.replyTo)) {
			msg.setReplyTo(InternetAddress.parse(this.replyTo, false));
		}
		msg.setSentDate(new Date());
		String recipients = this.to;
		msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
		return msg;
	}

	public String getTrimmedString(String s) {
		return s == null ? null : s.trim();
	}

	public String getSmtpHost() {
		return smtpHost;
	}
	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getReplyTo() {
		return replyTo;
	}
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
}
