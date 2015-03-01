package org.lumongo.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * Very similar to NamedThreadFactory in lucene but keeps the client from depending on lucene
 * 
 */
public class LumongoThreadFactory implements ThreadFactory {
	
	private final AtomicInteger threadCounter = new AtomicInteger(1);
	private final ThreadGroup threadGroup;
	private final String prefix;
	
	public LumongoThreadFactory(String prefix) {
		SecurityManager s = System.getSecurityManager();
		threadGroup = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		this.prefix = prefix;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(threadGroup, r, prefix + "-" + threadCounter.getAndIncrement(), 0);
		if (t.isDaemon()) {
			t.setDaemon(false);
		}
		if (t.getPriority() != Thread.NORM_PRIORITY) {
			t.setPriority(Thread.NORM_PRIORITY);
		}
		return t;
	}
}
