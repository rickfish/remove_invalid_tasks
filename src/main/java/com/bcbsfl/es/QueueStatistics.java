package com.bcbsfl.es;

import java.util.ArrayList;
import java.util.List;

public class QueueStatistics {
	public int count = 0;
	public List<String> messageIds = new ArrayList<String>();
	public void newMessageId(int maxMessageIds, String message) {
		increment();
		addMessageIdIfAppropriate(maxMessageIds, message);
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public void increment() {
		this.count++;
	}
	public List<String> getMessageIds() {
		return messageIds;
	}
	public void setMessageIds(List<String> messageIds) {
		this.messageIds = messageIds;
	}
	public void addMessageIdIfAppropriate(int maxMessageIds, String messageId) {
		if(this.messageIds.size() < maxMessageIds) {
			this.messageIds.add(messageId);
		}
	}
}
