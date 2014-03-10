package com.robby.hadoop.ipc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class IPCServer {

	public static final long IPC_VER = 1L;
	
	public static final int IPC_PORT = 32121;
	
	public static void main(String ...args) throws InterruptedException, IOException{
		IPCTestImpl  service = new IPCTestImpl();
//		Server s = RPC.getServer(service, "0.0.0.0", IPC_PORT, new Configuration());
		Server s = RPC.getServer(service, "0.0.0.0", IPC_PORT, 5,false,new Configuration());
		s.start();
		
		while(true){
			Thread.sleep(10000000);
		}
		
//		s.stop();
	}

}
