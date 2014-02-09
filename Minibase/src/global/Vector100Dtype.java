package global;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Vector100Dtype {
	int Max = 100;
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
	    		  
	    	  
	      }
		
	}
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

}