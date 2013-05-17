package org.lumongo.client.pool;

import java.util.List;

import javax.activity.InvalidActivityException;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.lumongo.client.command.GetMembers;
import org.lumongo.client.command.base.Command;
import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.Result;
import org.lumongo.cluster.message.Lumongo.IndexMapping;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class LumongoPool {

	protected class MembershipUpdateThread extends Thread {

		MembershipUpdateThread() {
			setDaemon(true);
			setName("LMMemberUpdateThread" + hashCode());
		}

		@Override
		public void run() {
			while (!isClosed) {
				try {
					try {
						Thread.sleep(memberUpdateInterval);
					} catch (InterruptedException e) {

					}
					updateMembers();

				} catch (Throwable t) {

				}
			}
		}
	}


	private List<LMMember> members;
	private int retries;
	private int maxIdle;
	private int maxConnections;
	private boolean routingEnabled;
	private boolean isClosed;
	private int memberUpdateInterval;

	private GenericKeyedObjectPool<LMMember, LumongoConnection> connectionPool;
	private IndexRouting indexRouting;


	public LumongoPool(final LumongoPoolConfig lumongoPoolConfig) throws Exception {
		members = lumongoPoolConfig.getMembers();
		retries = lumongoPoolConfig.getDefaultRetries();
		maxIdle = lumongoPoolConfig.getMaxIdle();
		maxConnections = lumongoPoolConfig.getMaxConnections();
		routingEnabled = lumongoPoolConfig.isRoutingEnabled();
		memberUpdateInterval = lumongoPoolConfig.getMemberUpdateInterval();
		if (memberUpdateInterval < 100) {
			//TODO think about cleaner ways to handle this
			throw new InvalidActivityException("Member update interval is less than the minimum of 100");
		}

		KeyedPoolableObjectFactory<LMMember, LumongoConnection> factory = new KeyedPoolableObjectFactory<LMMember, LumongoConnection>() {

			@Override
			public LumongoConnection makeObject(LMMember key) throws Exception {
				LumongoConnection lc = new LumongoConnection(key);
				lc.open(lumongoPoolConfig.isCompressedConnection());
				return lc;
			}

			@Override
			public void destroyObject(LMMember key, LumongoConnection obj) throws Exception {
				obj.close();
			}

			@Override
			public boolean validateObject(LMMember key, LumongoConnection obj) {
				return true;
			}

			@Override
			public void activateObject(LMMember key, LumongoConnection obj) throws Exception {

			}

			@Override
			public void passivateObject(LMMember key, LumongoConnection obj) throws Exception {

			}

		};
		GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config(); //
		poolConfig.maxIdle = maxIdle;
		poolConfig.maxActive = maxConnections;

		poolConfig.testOnBorrow = false;
		poolConfig.testOnReturn = false;

		connectionPool = new GenericKeyedObjectPool<LMMember, LumongoConnection>(factory, poolConfig);

		if (lumongoPoolConfig.isMemberUpdateEnabled()) {
			MembershipUpdateThread mut = new MembershipUpdateThread();
			mut.start();
		}
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void updateMembers(List<LMMember> members) {
		//TODO handle cleaning up out of the pool?
		this.members = members;
	}

	public void updateIndexMappings(List<IndexMapping> list) {
		indexRouting = new IndexRouting(list);
	}

	public void updateMembers() throws Exception {
		GetMembersResult getMembersResult = execute(new GetMembers());
		updateMembers(getMembersResult.getMembers());
		updateIndexMappings(getMembersResult.getIndexMappings());
	}

	public <R extends Result> R execute(Command<R> command) throws Exception {


		int tries = 0;
		while (true) {
			LumongoConnection lumongoConnection = null;
			LMMember selectedMember = null;
			try {
				boolean shouldRoute = (command instanceof RoutableCommand) && routingEnabled && (indexRouting != null);

				if (shouldRoute) {
					RoutableCommand rc = (RoutableCommand) command;
					selectedMember = indexRouting.getMember(rc.getIndexName(), rc.getUniqueId());
				}

				if (selectedMember == null) {
					List<LMMember> tempList = members; //stop array index out bounds on updates without locking
					int randomMemberIndex = (int) (Math.random() * tempList.size());
					selectedMember = tempList.get(randomMemberIndex);
				}


				lumongoConnection = connectionPool.borrowObject(selectedMember);

				R r = command.executeTimed(lumongoConnection);

				connectionPool.returnObject(selectedMember, lumongoConnection);
				return r;
			}
			catch (Exception e) {
				if (selectedMember != null && lumongoConnection != null) {
					try {
						connectionPool.invalidateObject(selectedMember, lumongoConnection);
					}
					catch (Exception e1) {
					}
				}
				if (tries >= retries) {
					throw e;
				}
				tries++;
			}
		}


	}

	public void close() throws Exception {
		connectionPool.close();
		isClosed = true;
	}

}
