package seri;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * java 序列化例子
 * @author hadoop
 *
 */
public class JavaSeri implements Serializable {

	private static final long serialVersionUID = -9070466689278306050L;

	public long a,b,c;

	public JavaSeri(long a, long b, long c) {
		super();
		this.a = a;
		this.b = b;
		this.c = c;
	}
	
	public static void main(String ...args) throws Exception{
		JavaSeri a = new JavaSeri(1L,2L,3L);
		
		FileOutputStream fos = new FileOutputStream("temp.out");
		ObjectOutputStream ojbos= new ObjectOutputStream(fos);
		ojbos.writeObject(a);
		ojbos.close();
		fos.close();
	}


}
