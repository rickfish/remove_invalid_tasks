package com.bcbsfl.es;

public class TaskSearchScrolling extends TaskPurgerApp {

	public TaskSearchScrolling() {
	}

	public void scroll() throws Exception {
		int iter = 0;
		while(true) {
			System.out.println("******************** STARTING ITERATION " + ++iter);
			for(int i = 0; i < 60000; i += 1000) {
				this.runTaskSearch(i,  1000,  "startTime",  "taskDefName%3ASSO_RPM_UNIVERSAL_WAIT_LOOP_TASK%20AND%20status%3AIN_PROGRESS");
			}
			System.out.println("******************** FINISHED ITERATION " + ++iter + ", waiting 30 seconds for next iteration");
			Thread.sleep(30000);
		}
	}
}