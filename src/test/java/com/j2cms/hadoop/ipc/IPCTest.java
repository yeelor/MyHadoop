package com.j2cms.hadoop.ipc;

import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface IPCTest extends VersionedProtocol {

	abstract public int add(Text s);
	
	abstract public int del(Text s);

	abstract public Set<Text> getClusters();
	
	abstract public int getSize();

}
