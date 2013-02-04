package com.basho.proserv.datamigrator.util;

import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
	private String name = null;
	
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r, this.name); 
		thread.setDaemon(false);
		return thread;
	}
	
	public void setNextThreadName(String name) {
		this.name = name;
	}

}
