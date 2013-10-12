package org.lumongo.client.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lumongo.util.LumongoThreadFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class WorkPool {
	
	private ListeningExecutorService pool;
	private final static AtomicInteger threadNumber = new AtomicInteger(1);
	
	public WorkPool(int threads) {
		this(threads, threads * 10);
	}
	
	public WorkPool(int threads, int maxQueued) {
		this(threads, maxQueued, "workPool-" + threadNumber.getAndIncrement());
	}
	
	public WorkPool(int threads, int maxQueued, String poolName) {
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(maxQueued) {
			private static final long serialVersionUID = 1L;
			
			@Override
			public boolean offer(Runnable e) {
				try {
					put(e);
				}
				catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
				return true;
			};
		};
		
		pool = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue, new LumongoThreadFactory(
						poolName)));
	}
	
	public <T> ListenableFuture<T> executeAsync(Callable<T> task) {
		return pool.submit(task);
	}
	
	public <T> T execute(Callable<T> task) throws Exception {
		try {
			return executeAsync(task).get();
		}
		catch (InterruptedException e) {
			throw e;
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause != null) {
				if (cause instanceof Exception) {
					throw (Exception) cause;
				}
			}
			throw e;
		}
	}
	
	public void shutdown() throws Exception {
		pool.shutdown();
		boolean terminated = false;
		try {
			while (!terminated) {
				// terminates immediately on completion
				terminated = pool.awaitTermination(1, TimeUnit.HOURS);
			}
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
