package seri;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Hadoop序列化的复杂例子
 * @author hadoop
 *
 */
public class Complex implements Writable {

	
	public List<String> l;
	
	
	
	public Complex() {
		super();
	}

	public Complex(List<String> l) {
		super();
		this.l = l;
	}


	public static void main(String ...args) throws Exception{
		List<String> l = new ArrayList<String>();
		l.add("123");
		l.add("456");
		
		Complex a = new Complex(l);
		FileOutputStream fos = new FileOutputStream("temp.out");
		DataOutputStream dos= new DataOutputStream(fos);
		a.write(dos);
		dos.close();
		fos.close();
		
		//反序列化
		FileInputStream fin = new FileInputStream("temp.out");
		DataInputStream dis= new DataInputStream(fin);
		Complex b = new Complex();
		b.readFields(dis);
		dis.close();
		fin.close();
		
		for(String s:b.l){
			System.out.println(s);
		}
		
		
	}

	

	/**
	 * 反序列化，读入
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int n = in.readInt();
		if(l==null){
			l = new ArrayList<String>();
		}
		for(int i=0;i<n;i++){
			l.add(in.readLine());
		}
	}
	
	/**
	 * 序列入，写入
	 */
	@Override
	public void write(DataOutput out) throws IOException {
	
		out.writeInt(l.size());
		for(String s:l){
			out.writeBytes(s);
			out.writeBytes("\n");
		}
	}


}
