package org.lumongo.server.connection;

import java.io.IOException;

import org.lumongo.cluster.message.Lumongo.InternalService;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;

public class InternalRpcConnection {

	private InternalService.BlockingInterface service;
	private RpcClient rpcClient;
	private DuplexTcpClientBootstrap bootstrap;
	private RpcController rpcController;

	public InternalRpcConnection(InternalService.BlockingInterface service, RpcClient rpcClient, DuplexTcpClientBootstrap bootstrap) {
		this.service = service;
		this.rpcClient = rpcClient;
		this.bootstrap = bootstrap;
		rpcController = null;
	}

	public InternalService.BlockingInterface getService() {
		return service;
	}

	public RpcController getClientRPCController() throws IOException {
		if (rpcClient != null) {
			rpcController = (ClientRpcController) rpcClient.newRpcController();
			return rpcController;
		}
		throw new IOException("Connection is not open");
	}

	public DuplexTcpClientBootstrap getBootstrap() {
		return bootstrap;
	}

	public void close() {
		try {
			rpcController.startCancel();
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		try {
			if (rpcClient != null) {
				System.out.println("Closing connection to " + rpcClient.getPeerInfo());
				rpcClient.close();
			}
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		rpcClient = null;
		try {
			if (bootstrap != null) {
				bootstrap.releaseExternalResources();
			}
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		bootstrap = null;
		service = null;
	}
}
