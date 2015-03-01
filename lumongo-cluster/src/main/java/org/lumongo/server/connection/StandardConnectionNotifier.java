package org.lumongo.server.connection;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import org.apache.log4j.Logger;

public class StandardConnectionNotifier implements TcpConnectionEventListener {

	private final Logger log;

	public StandardConnectionNotifier(Logger log) {
		this.log = log;
	}

	@Override
	public void connectionOpened(RpcClientChannel clientChannel) {
		log.info("connectionOpened: " + clientChannel);
	}

	@Override
	public void connectionClosed(RpcClientChannel clientChannel) {
		log.info("connectionClosed: " + clientChannel);
	}
}
