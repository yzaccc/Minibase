package global;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Zongkun
 * task 1
 * phrase 2
 * @author akun1012
 *
 */
public class Vector100Dtype {
	public static int  Max = 100;
	private short[] VectorValue = new short[Max];
	
	public Vector100Dtype(byte[] data) throws IOException {
		  InputStream in;
	      DataInputStream instr;
	      short value;
	      for(int i =0;i<Max;i++){
	    	  byte tmp[] = new byte[2];
	    	  System.arraycopy (data, i*2, tmp, 0, 2);
	    	  in = new ByteArrayInputStream(tmp);
	    	  instr = new DataInputStream(in);
	    	  value = instr.readShort();
	    	  VectorValue[i] = value;
	      }
		
	}
	public Vector100Dtype(short[] vectorValue){
		VectorValue = vectorValue;
	}
	public static Vector100Dtype getMaxVector100D(Vector100Dtype v){
		short [] maxarray= {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		
		
		short []  targetValue = v.getVectorValue();
		for(int i =0;i<Max;i++){
			if (targetValue[i] >0 )
				maxarray[i] = -10000;
			else
				maxarray[i] = 10000;
		}
		Vector100Dtype maxv = new Vector100Dtype(maxarray);
		return maxv;
	}
	public void printVector(){
		for (int i=0;i<Max;i++){
			System.out.print(VectorValue[i]);
			
		}
		System.out.println();
	}
	
	
	
	/**
	 * Zongkun
	 * task 1
	 * @param v
	 * @return
	 * @throws java.io.IOException
	 */
	public static byte [] Vector100DtoByte(Vector100Dtype v) 
			throws java.io.IOException {
		OutputStream out = new ByteArrayOutputStream();
	    DataOutputStream outstr = new DataOutputStream (out);
	    for (int i=0;i<100;i++) {
	    	outstr.writeShort(v.VectorValue[i]);
	    }
	    byte []B = ((ByteArrayOutputStream) out).toByteArray();
	    return B;
	}
	

	/**
	 * Zongkun MengYang
	 * Calculate the distance between the vectors.
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static int distance(Vector100Dtype d1,Vector100Dtype d2){
		short[] d1short = d1.getVectorValue();
		short[] d2short = d2.getVectorValue();
		double s = 0;
		int distance = 0;
		for(int i = 0;i< 100;i++){
			s += Math.pow(d1short[i]-d2short[i],2);
		}
		distance = (int)Math.sqrt(s);
		return distance;
	}

	/**
	 * Zongkun
	 * getter setter
	 * @param v
	 * @return
	 */
	public short[] getVectorValue() {
		return VectorValue;
	}
	
	public void setVectorValue(short[] vectorValue) {
		VectorValue = vectorValue;
	}
}