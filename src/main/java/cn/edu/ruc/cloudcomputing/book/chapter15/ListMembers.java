package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

public class ListMembers extends ZooKeeperInstance {

	public void list(String groupPath) throws Exception{
		//获取所有子节点
		List<String> children = zk.getChildren(groupPath, false);
		if(children.isEmpty()){
			System.out.println("组"+groupPath +"中没有组成员存在 !");
		}
		for(String child:children){
			System.out.println(child);
		}
	}
	
	public static void main(String[] args) throws Exception {

		ListMembers lm = new ListMembers();
		lm.createZKInstance();
		
		long t1 = System.currentTimeMillis();//System.nanoTime();
		lm.list("/ZKGroup");
		long t2 = System.currentTimeMillis();
		System.out.println("time takes "+NumberFormat.getInstance(Locale.CHINA).format(t2-t1)+" ms");
		lm.closeZK();
		
		
	}

}
