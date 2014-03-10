package com.robby.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class IPCClient {
	public static void main(String... args) throws IOException {
		
		String server = "127.0.0.1";
		
		InetSocketAddress addr = new InetSocketAddress(server, IPCServer.IPC_PORT);
		
		IPCTest query  = (IPCTest)RPC.getProxy(IPCTest.class, IPCServer.IPC_VER, addr, new Configuration()) ;
		System.out.println(query.add(1, 2));
		
		RPC.stopProxy(query);
		
	}

}
