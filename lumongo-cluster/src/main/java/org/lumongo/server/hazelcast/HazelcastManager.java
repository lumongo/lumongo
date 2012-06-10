package org.lumongo.server.hazelcast;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.Nodes.HazelcastNode;
import org.lumongo.server.exceptions.IndexDoesNotExist;
import org.lumongo.server.indexing.IndexManager;

import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.GroupProperties;

public class HazelcastManager implements MembershipListener, LifecycleListener {
	private final static Logger log = Logger.getLogger(HazelcastManager.class);
	
	private LocalNodeConfig localNodeConfig;
	private IndexManager indexManager;
	
	private final ReadWriteLock initLock;
	
	private HazelcastInstance hazelcastInstance;
	private Member self;
	
	private final static Map<Integer, HazelcastManager> portToHazelcastManagerMap = new HashMap<Integer, HazelcastManager>();
	
	public static HazelcastManager createHazelcastManager(LocalNodeConfig localNodeConfig, IndexManager indexManager, Set<HazelcastNode> nodes,
			String hazelcastName) throws Exception {
		HazelcastManager hazelcastManager = new HazelcastManager(localNodeConfig, indexManager);
		portToHazelcastManagerMap.put(hazelcastManager.getHazelcastPort(), hazelcastManager);
		indexManager.init(hazelcastManager);
		hazelcastManager.init(nodes, hazelcastName);
		return hazelcastManager;
	}
	
	public static HazelcastManager getHazelcastManager(int port) {
		return portToHazelcastManagerMap.get(port);
	}
	
	private HazelcastManager(LocalNodeConfig localNodeConfig, IndexManager indexManager) {
		this.initLock = new ReentrantReadWriteLock(true);
		initLock.writeLock().lock();
		this.indexManager = indexManager;
		this.localNodeConfig = localNodeConfig;
	}
	
	public int getHazelcastPort() {
		return localNodeConfig.getHazelcastPort();
	}
	
	public Member getSelf() {
		return self;
	}
	
	public void init(Set<HazelcastNode> nodes, String hazelcastName) throws Exception {
		
		// force Hazelcast to use log4j
		System.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");
		// disable Hazelcast shutdown hook to allow LuMongo to handle
		System.setProperty(GroupProperties.PROP_SHUTDOWNHOOK_ENABLED, "false");
		
		System.setProperty(GroupProperties.PROP_REST_ENABLED, "false");
		
		int hazelcastPort = localNodeConfig.getHazelcastPort();
		String groupName = hazelcastName;
		String groupPassword = hazelcastName;
		
		Config cfg = new Config();
		cfg.getGroupConfig().setName(groupName);
		cfg.getGroupConfig().setPassword(groupPassword);
		cfg.setPortAutoIncrement(false);
		cfg.setPort(hazelcastPort);
		cfg.setInstanceName("" + hazelcastPort);
		
		cfg.getManagementCenterConfig().setEnabled(false);
		
		NetworkConfig network = cfg.getNetworkConfig();
		Join join = network.getJoin();
		
		join.getMulticastConfig().setEnabled(false);
		join.getTcpIpConfig().setEnabled(true);
		for (HazelcastNode node : nodes) {
			join.getTcpIpConfig().addMember(node.getAddress() + ":" + node.getHazelcastPort());
		}
		
		hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
		self = hazelcastInstance.getCluster().getLocalMember();
		
		hazelcastInstance.getCluster().addMembershipListener(this);
		hazelcastInstance.getLifecycleService().addLifecycleListener(this);
		
		log.info("Initialized hazelcast");
		Set<Member> members = hazelcastInstance.getCluster().getMembers();
		
		Member firstMember = members.iterator().next();
		
		if (firstMember.equals(self)) {
			log.info("Member is owner of cluster");
			indexManager.loadIndexes();
		}
		
		log.info("Current cluster members: <" + members + ">");
		indexManager.openConnections(members);
		
		initLock.writeLock().unlock();
		
	}
	
	public void shutdown() {
		hazelcastInstance.getLifecycleService().kill();
	}
	
	@Override
	public void memberAdded(MembershipEvent membershipEvent) {
		Set<Member> members = membershipEvent.getCluster().getMembers();
		Member memberAdded = membershipEvent.getMember();
		log.info("Added member: <" + membershipEvent.getMember() + "> Current members: <" + members + ">");
		
		Member firstMember = members.iterator().next();
		boolean master = self.equals(firstMember);
		
		try {
			indexManager.handleServerAdded(members, memberAdded, master);
		}
		catch (Exception e) {
			log.error(e.getClass().getSimpleName() + ": ", e);
		}
	}
	
	@Override
	public void memberRemoved(MembershipEvent membershipEvent) {
		
		Set<Member> members = membershipEvent.getCluster().getMembers();
		Member memberRemoved = membershipEvent.getMember();
		log.info("Lost member: <" + memberRemoved + "> Current members: <" + members + ">");
		
		Member firstMember = membershipEvent.getCluster().getMembers().iterator().next();
		
		boolean master = self.equals(firstMember);
		
		indexManager.handleServerRemoved(members, memberRemoved, master);
		
	}
	
	@Override
	public void stateChanged(LifecycleEvent event) {
		log.info("Hazelcast has new state: " + event);
	}
	
	public void updateSegmentMap(String indexName, Map<Member, Set<Integer>> newMemberToSegmentMap) throws Exception {
		initLock.readLock().lock();
		try {
			log.info("Updating segment map for index <" + indexName + ">: new segments: " + newMemberToSegmentMap);
			indexManager.updateSegmentMap(indexName, newMemberToSegmentMap);
		}
		finally {
			initLock.readLock().unlock();
		}
	}
	
	public Set<Member> getMembers() {
		return hazelcastInstance.getCluster().getMembers();
	}
	
	public ILock getLock(String lockName) {
		return hazelcastInstance.getLock(lockName);
	}
	
	public void reloadIndexSettings(String indexName) throws Exception {
		indexManager.reloadIndexSettings(indexName);
	}
	
	public ExecutorService getExecutorService() {
		return hazelcastInstance.getExecutorService();
	}
	
	public void unloadIndex(String indexName) throws CorruptIndexException, IndexDoesNotExist, IOException {
		indexManager.unloadIndex(indexName);
		
	}
	
}
