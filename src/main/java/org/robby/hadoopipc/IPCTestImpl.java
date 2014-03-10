package org.robby.hadoopipc;

import java.io.IOException;

public class IPCTestImpl implements IPCTest{

	

	@Override
	public int add(int a, int b) {
		// TODO Auto-generated method stub
		return a+b;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		// TODO Auto-generated method stub
		return IPCServer.IPC_VER;
	}

}
