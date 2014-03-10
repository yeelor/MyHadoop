package seri;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Hadoop序列化的例子
 * @author hadoop
 *
 */
public class HadoopSeri implements Writable {

	public long a,b,c;

	
	public HadoopSeri() {
		super();
	}

	public HadoopSeri(long a, long b, long c) {
		super();
		this.a = a;
		this.b = b;
		this.c = c;
	}
	
	public static void main(String ...args) throws Exception{
		
		HadoopSeri a = new HadoopSeri(1L,2L,3L);
		FileOutputStream fos = new FileOutputStream("temp.out");
		DataOutputStream dos= new DataOutputStream(fos);
		a.write(dos);
		dos.close();
		fos.close();
		
		//反序列化
		FileInputStream fin = new FileInputStream("temp.out");
		DataInputStream dis= new DataInputStream(fin);
		HadoopSeri b = new HadoopSeri();
		b.readFields(dis);
		dis.close();
		fin.close();
		
		System.out.println(b.a+","+b.b+","+b.c);
		
	}

	/**
	 * 序列入，写入
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(a);
		out.writeLong(b);
		out.writeLong(c);
	}

	/**
	 * 反序列化，读入
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.a=in.readLong();
		this.b=in.readLong();
		this.c=in.readLong();
	}


}
