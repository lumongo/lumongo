package org.lumongo.server.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ILock;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import org.apache.log4j.Logger;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.Nodes.HazelcastNode;
import org.lumongo.server.index.LumongoIndexManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HazelcastManager implements MembershipListener, LifecycleListener {
	private final static Logger log = Logger.getLogger(HazelcastManager.class);
	
	private LocalNodeConfig localNodeConfig;
	private LumongoIndexManager indexManager;
	
	private final ReadWriteLock initLock;
	
	private HazelcastInstance hazelcastInstance;
	private Member self;

	private final static Map<Integer, HazelcastManager> portToHazelcastManagerMap = new HashMap<>();
	
	public static HazelcastManager createHazelcastManager(LocalNodeConfig localNodeConfig, LumongoIndexManager indexManager, Set<HazelcastNode> nodes,
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
	
	private HazelcastManager(LocalNodeConfig localNodeConfig, LumongoIndexManager indexManager) {
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
		
		Config cfg = new Config();
		cfg.getGroupConfig().setName(hazelcastName);
		cfg.getGroupConfig().setPassword(hazelcastName);
		cfg.getNetworkConfig().setPortAutoIncrement(false);
		cfg.getNetworkConfig().setPort(hazelcastPort);
		cfg.setInstanceName("" + hazelcastPort);
		
		cfg.getManagementCenterConfig().setEnabled(false);
		
		NetworkConfig network = cfg.getNetworkConfig();
		JoinConfig joinConfig = network.getJoin();
		
		joinConfig.getMulticastConfig().setEnabled(false);
		joinConfig.getTcpIpConfig().setEnabled(true);
		for (HazelcastNode node : nodes) {
			joinConfig.getTcpIpConfig().addMember(node.getAddress() + ":" + node.getHazelcastPort());
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
		//TODO should this be shutdown?
		hazelcastInstance.getLifecycleService().terminate();
		portToHazelcastManagerMap.remove(getHazelcastPort());

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
	public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
		
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
	
	public IExecutorService getExecutorService() {
		return hazelcastInstance.getExecutorService("default");
	}

	public void unloadIndex(String indexName, boolean terminate) throws IOException {
		indexManager.unloadIndex(indexName, terminate);
	}
	
	public long getClusterTime() {
		return hazelcastInstance.getCluster().getClusterTime();
	}
	
}
