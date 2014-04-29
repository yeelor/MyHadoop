package com.j2cms.hadoop.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

public class IPCTestImpl implements IPCTest {

//	public static List<Text> clusters = new ArrayList<Text>();
	public static Set<Text> clusters = new HashSet<Text>();
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return IPCServer.IPC_VER;
	}

	@Override
	public int add(Text s) {
		clusters.add(s);
		return clusters.size();
	}
	
	@Override
	public int del(Text s) {
		clusters.remove(s);
		return clusters.size();
	}

	//这里要序列化才能传输
	@Override
	public Set<Text> getClusters() {
		return clusters;
	}
	
	public int getSize(){
		return clusters.size();
	}
	
}
