package com.bcbsfl.es;

public class Main {

	public static void main(String[] args) {
		Utils.init();
		String appToRun = Utils.getProperty("which.app");
		System.out.println("Running " + appToRun);
		try {
			switch(appToRun) {
			case "taskPurger":
				new TaskPurger().purgeTasks();
				break;
			case "queueMessagePurger":
				new QueueMessagePurger().purgeQueueMessages();
				break;
			case "queueMessageInfo":
				new QueueMessageInfoCollector().collect();
				break;
			case "careGapInfo":
				new CareGapInfoCollector().collect();
				break;
			case "rpmWorkflowTerminator":
				new RpmWorkflowTerminator().terminate();
				break;
			case "rpmTaskTerminator":
				new RpmTaskTerminator().terminate();
				break;
			case "rpmQueueMessageDeleter":
				new RpmQueueMessageDeleter().delete();
				break;
			case "rpmTaskInfoCollector":
				new RpmTaskInfoCollector().collect();
				break;
			case "rpmWorkflowInfoCollector":
				new RpmWorkflowInfoCollector().collect();
				break;
			case "tesWorkflowTerminator":
				new TesWorkflowTerminator().terminate();
				break;
			case "rpmOneTaskInfoCollector":
				new RpmOneTaskInfoCollector().collect();
				break;
			case "rpmOldWorkfowTerminator":
				new RpmOldWorkflowTerminator().terminate();
				break;
			case "rpmOldWorkfowTaskTerminator":
				new RpmOldWorkflowTaskTerminator().terminate();
				break;
			case "rpmTerminatorByCorrId":
				new RpmTerminatorByCorrId().terminate();
				break;
			case "rpmTerminateTasksByCorrId":
				new RpmTerminateTasksByCorrId().terminate();
				break;
			case "rpmOldWorkflowAging":
				new RpmOldWorkflowAging().report();
				break;
			case "rpmFindStrangeSituations":
				new RpmFindStrangeSituations().find();
				break;
			case "cwfWorkflowTerminator":
				new CwfWorkflowTerminator().terminate();
				break;
			case "cwfQueueMessageTaskTerminator":
				new CwfQueueMessageTaskTerminator().terminate();
				break;
			case "cdtClearWorkflows":
				new CdtClearWorkflows().clear();
				break;
			case "nonRpmUnack":
				new NonRpmUnack().unack();
				break;
			case "waitTaskInfoCollector":
				new WaitTaskInfoCollector().collect();
				break;
			case "taskSearchScrolling":
				new TaskSearchScrolling().scroll();
				break;
			case "removeOldWorkflows":
				new RemoveOldWorkflows().remove();
				break;
			case "queueMessageTaskTerminator":
				new QueueMessageTaskTerminator().terminate();
				break;
			case "queueMessageWorkflowTerminator":
				new QueueMessageWorkflowTerminator().terminate();
				break;
			case "queueMessageTaskWorkflowTerminator":
				new QueueMessageTaskWorkflowTerminator().terminate();
				break;
			default:
				System.out.println("******* The which.app variable is set to '" + appToRun + "' which is not a valid option.");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		while(true) {
			System.out.println("Since it is possible that we are running in Openshift, we will keep sleeping for an hour to keep the pod running...");
			try {
				Thread.sleep(3600000);
			} catch(Exception e) {
			}
		}
	}
}
