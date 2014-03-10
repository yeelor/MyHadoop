package com.robby.hadoop.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IPCTest extends VersionedProtocol {

	int add(int a,int b);
}
