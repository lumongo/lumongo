package org.lumongo.server.connection;

import org.apache.log4j.Logger;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

public class StandardConnectionNotifier extends RpcConnectionEventNotifier {
	
	private final Logger log;
	
	private class StandardConnectionListener implements RpcConnectionEventListener {
		
		public StandardConnectionListener() {
			
		}
		
		@Override
		public void connectionReestablished(RpcClientChannel clientChannel) {
			log.info("connectionReestablished: " + clientChannel);
		}
		
		@Override
		public void connectionOpened(RpcClientChannel clientChannel) {
			log.info("connectionOpened: " + clientChannel);
		}
		
		@Override
		public void connectionLost(RpcClientChannel clientChannel) {
			log.info("connectionLost: " + clientChannel);
		}
		
		@Override
		public void connectionChanged(RpcClientChannel clientChannel) {
			log.info("connectionChanged: " + clientChannel);
		}
	}
	
	public StandardConnectionNotifier(Logger log) {
		this.log = log;
		setEventListener(new StandardConnectionListener());
	}
}
