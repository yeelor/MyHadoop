package com.j2cms.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;

//public class Main {
//    public static void main(String[] args) {
//        Scanner in = new Scanner(System.in);
//        while(in.hasNext()) {
//               int a = in.nextInt();
//               int b = in.nextInt();
//               System.out.println(a + b);
//        }
//    }
//}

public class IPCClient {
	public static void main(String... args) throws IOException {

//		String server = "lenovo0";
		String server = "218.193.154.135";
		InetSocketAddress addr = new InetSocketAddress(server, IPCServer.IPC_PORT);
		IPCTest query = (IPCTest) RPC.getProxy(IPCTest.class, IPCServer.IPC_VER, addr, new Configuration());

		Scanner in = new Scanner(System.in);
		while (in.hasNext()) {
			String a = in.nextLine();//.next();
			if (a.equalsIgnoreCase("exit"))
				break;
			long time1 = System.currentTimeMillis();
			int size;
			if(a.startsWith("del")){
				a = a.split(" ")[1];
				size = query.del(new Text(a));
			}else {
				size = query.add(new Text(a));
			}
			long time2 = System.currentTimeMillis();
			System.out.println("time cost: "+(time2-time1)+" ms size :"+size);
		}
		Set<Text> clusters = query.getClusters();
		System.out.println("There are "+query.getSize()+" instances in clusters:");
		for(Text s:clusters){
			System.out.print(s+"\t");
		}
		RPC.stopProxy(query);
	}

}
