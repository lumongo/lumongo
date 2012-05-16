package org.lumongo.server.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.lumongo.server.hazelcast.HazelcastManager;

import com.hazelcast.core.Member;

public abstract class SocketRequestFederator<I, O> {
	
	private HazelcastManager hazelcastManager;
	private ExecutorService pool;
	
	public SocketRequestFederator(HazelcastManager hazelcastManager, ExecutorService pool) {
		this.hazelcastManager = hazelcastManager;
		this.pool = pool;
	}
	
	public List<O> send(final I request) throws Exception {
		Set<Member> members = hazelcastManager.getMembers();
		
		List<Future<O>> futureResponses = new ArrayList<Future<O>>();
		
		final Member self = hazelcastManager.getSelf();
		
		for (final Member m : members) {
			
			Future<O> futureResponse = pool.submit(new Callable<O>() {
				
				@Override
				public O call() throws Exception {
					
					if (!self.equals(m)) {
						return processExternal(m, request);
					}
					
					return processInternal(request);
					
				}
				
			});
			
			futureResponses.add(futureResponse);
		}
		
		ArrayList<O> results = new ArrayList<O>();
		for (Future<O> response : futureResponses) {
			try {
				O result = response.get();
				results.add(result);
			}
			catch (InterruptedException e) {
				throw new Exception("Interrupted while waiting for results");
			}
			catch (Exception e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw e;
				}
				
				throw e;
			}
		}
		
		return results;
		
	}
	
	public abstract O processExternal(Member m, I request) throws Exception;
	
	public abstract O processInternal(I request) throws Exception;
}
