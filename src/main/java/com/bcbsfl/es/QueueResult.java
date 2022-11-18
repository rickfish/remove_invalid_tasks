package com.bcbsfl.es;

public class QueueResult {
	String queue = null;
	boolean isWorkflow = true;
	String workflowInstanceId = null;
	String taskReferenceName = null;
	boolean rpmObject = false;
	boolean unacked = false;
	boolean notInProgress = false;
	boolean notPopped = false;
}
