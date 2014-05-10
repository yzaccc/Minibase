package global;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import VAIndex.VAFile;

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
	public Vector100Dtype(short value){
		for (int i=0;i<Max;i++){
			VectorValue[i] = value;
		}
	}
	
	public Vector100Dtype(short[] vectorValue){
		for (int i=0;i<100;i++)
		{
			this.VectorValue[i] = vectorValue[i];
			
		}
	}
	

	public static Vector100Dtype getMaxVector100D(Vector100Dtype v){
		//short [] maxarray= {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		Vector100Dtype vector = new Vector100Dtype((short)0);
		
		short []  targetValue = v.getVectorValue();
		for(int i =0;i<Max;i++){
			if (targetValue[i] >0 )
				vector.VectorValue[i] = -10000;
			else
				vector.VectorValue[i] = 10000;
		}
		Vector100Dtype maxv = new Vector100Dtype(vector.VectorValue);
		return maxv;
	}
	public void printVector(){
		for (int i=0;i<Max;i++){
			//System.out.println(i+" "+VectorValue[i]+" ");
			System.out.print(VectorValue[i]+" ");
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
	
	public static double distance2(Vector100Dtype d1,Vector100Dtype d2){
		short[] d1short = d1.getVectorValue();
		short[] d2short = d2.getVectorValue();
		double s = 0;
		double distance = 0;
		for(int i = 0;i< 100;i++){
			s += Math.pow(d1short[i]-d2short[i],2);
		}
		distance = Math.sqrt(s);
		return distance;
	}
	/**
	 *  for phase 3
	 *  return the lower bound vector from a distance
	 * @param v
	 * @param distance
	 * @return
	 */
	public static  Vector100Dtype getLowerBoundVector(Vector100Dtype v, int distance,int b){
		//Vector100Dtype low_vec = new Vector100Dtype(v);
		int psize = 20000 / (1<<b) ; //partition size
		short[] d1short = new short[Max];
		short[] d2short = v.getVectorValue();
		for (int i=0;i<100;i++){
			d1short[i] = d2short[i];
		}
		int d;
		int dsq = distance * distance; //square of distance 
		for (int i = 0; i < 100;i++){
			d = d1short[i] - VAFile.LOWERBOUND;
			if (d*d >= dsq){ // stop here
				d1short[i] = (short)(d1short[i] - (short)Math.sqrt(dsq));
				
				break;
			}
			else{
				d1short[i] = VAFile.LOWERBOUND;
				dsq = dsq - d*d;
			}
			
		}
		for (int i = 0; i < 100;i++){
			if (d1short[i] - psize >= -10000){
				d1short[i] = (short)(d1short[i] - psize);
				break;
			}
		}
		Vector100Dtype low_vec = new Vector100Dtype(d1short);
		return low_vec;
	}
	/**
	 *  for phase 3
	 * @param v
	 * @param distance
	 * @return
	 */
	
	public static  Vector100Dtype getUpperBoundVector(Vector100Dtype v, int distance,int b){
		//Vector100Dtype low_vec = new Vector100Dtype(v);
		int psize = 20000 / (1<<b) ; //partition size
		short[] d1short = new short[Max];
		short[] d2short = v.getVectorValue();
		for (int i=0;i<100;i++){
			d1short[i] = d2short[i];
		}
		int d;
		int dsq = distance * distance; //square of distance 
		for (int i = 0; i < 100;i++){
			d = VAFile.UPPERBOUND - d1short[i];
			if (d*d >= dsq){ // stop here
				d1short[i] = (short)(d1short[i] + (short)Math.sqrt(dsq));
				break;
			}
			else{
				d1short[i] = VAFile.UPPERBOUND;
				dsq = dsq + d*d;
			}
			
		}
		for (int i = 0; i < 100;i++){
			if (d1short[i] + psize <= 10000){
				d1short[i] = (short)(d1short[i] + psize);
				break;
			}
		}
		Vector100Dtype low_vec = new Vector100Dtype(d1short);
		return low_vec;
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
	public short getVectorValueAt(int idx) {
		return VectorValue[idx];
	}
	
	public void setVectorValue(short[] vectorValue) {
		VectorValue = vectorValue;
	}
}