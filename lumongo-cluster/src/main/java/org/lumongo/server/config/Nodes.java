package org.lumongo.server.config;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.hazelcast.core.Member;

public class Nodes {
	
	public static class HazelcastNode {
		private String address;
		private int hazelcastPort;
		
		public HazelcastNode(String address, int hazelcastPort) {
			this.address = address;
			this.hazelcastPort = hazelcastPort;
		}
		
		public String getAddress() {
			return address;
		}
		
		public int getHazelcastPort() {
			return hazelcastPort;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((address == null) ? 0 : address.hashCode());
			result = prime * result + hazelcastPort;
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null || !(obj instanceof HazelcastNode))
				return false;
			
			HazelcastNode other = (HazelcastNode) obj;
			
			if (address == null) {
				if (other.address != null)
					return false;
			}
			else if (!address.equals(other.address))
				return false;
			if (hazelcastPort != other.hazelcastPort)
				return false;
			return true;
		}
		
	}
	
	private HashMap<HazelcastNode, LocalNodeConfig> nodes;
	
	public Nodes() {
		nodes = new HashMap<HazelcastNode, LocalNodeConfig>();
	}
	
	public void add(String address, LocalNodeConfig lnc) {
		
		nodes.put(new HazelcastNode(address, lnc.getHazelcastPort()), lnc);
	}
	
	protected String formKey(String address, int hazelPort) {
		return address + "-" + hazelPort;
	}
	
	public Set<HazelcastNode> getHazelcastNodes() {
		return new HashSet<HazelcastNode>(nodes.keySet());
	}
	
	public LocalNodeConfig find(Member member) throws Exception {
		InetAddress inetAddress = member.getSocketAddress().getAddress();
		
		String memberIp = inetAddress.getHostAddress();
		String fullHostName = inetAddress.getCanonicalHostName();
		
		int hazelcastPort = member.getSocketAddress().getPort();
		
		Set<HazelcastNode> matches = new HashSet<HazelcastNode>();
		matches.add(new HazelcastNode(memberIp, hazelcastPort));
		matches.add(new HazelcastNode(fullHostName, hazelcastPort));
		
		for (HazelcastNode node : nodes.keySet()) {
			if (matches.contains(node)) {
				LocalNodeConfig localNodeConfig = nodes.get(node);
				return localNodeConfig;
			}
			
		}
		
		throw new Exception("Member with memberIp <" + memberIp + "> and fullHostName <" + fullHostName + "> and hazelcast port <" + hazelcastPort
						+ "> not found in cluster membership.  Correctly register the machine with server address or ip that all machines can resolve");
		
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Nodes [");
		for (HazelcastNode node : nodes.keySet()) {
			LocalNodeConfig lnc = nodes.get(node);
			sb.append("\n  ");
			sb.append(node.getAddress());
			sb.append(" : ");
			sb.append(lnc);
		}
		sb.append("\n]");
		return sb.toString();
	}
}
