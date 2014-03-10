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
 * Complex2里含Complex
 * @author hadoop
 *
 */
public class Complex2 implements Writable {

	public long a ;
	public Complex b ;
	
	
	public Complex2() {
		a = 0L;
		b = null;
	}



	public Complex2(long a, Complex b) {
		super();
		this.a = a;
		this.b = b;
	}



	public static void main(String ...args) throws Exception{
		List<String> l = new ArrayList<String>();
		l.add("123");
		l.add("456");
		
		
		Complex a = new Complex(l);
		
		Complex2 a1 = new Complex2(1L,a);
		
		
		FileOutputStream fos = new FileOutputStream("temp.out");
		DataOutputStream dos= new DataOutputStream(fos);
		a1.write(dos);
		dos.close();
		fos.close();
		
		//反序列化
		FileInputStream fin = new FileInputStream("temp.out");
		DataInputStream dis= new DataInputStream(fin);
		Complex2 b = new Complex2();
		b.readFields(dis);
		dis.close();
		fin.close();
		
		System.out.println(b.a);
		for(String s:b.b.l){
			System.out.println(s);
		}
		
		
	}

	

	/**
	 * 反序列化，读入
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.a = in.readLong();
		if(b ==null){
			b = new Complex();
		}
		b.readFields(in);
	}
	
	/**
	 * 序列入，写入
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(a);
		b.write(out);//b已经实现了
	}


}
