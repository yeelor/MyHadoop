package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperInstance {

	public static final int SEESION_TIMEOUT = 30000;
	
	ZooKeeper zk;
	
	Watcher wh = new Watcher(){
		public void process(WatchedEvent event){
			System.out.println(event.toString());
		}
	};
	
	public void createZKInstance() throws IOException{
		zk = new ZooKeeper("127.0.0.1:2181",ZooKeeperInstance.SEESION_TIMEOUT,this.wh);
	}
	
	public void closeZK() throws InterruptedException{
		zk.close();
	}

}
