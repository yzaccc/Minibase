package global;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;

public class Vector100Dtype {
	private short[] V100 = new short[100];
	
	public Vector100Dtype() {
		
	}
	public byte [] Vector100DtoByte() 
			throws java.io.IOException {
		OutputStream out = new ByteArrayOutputStream();
	    DataOutputStream outstr = new DataOutputStream (out);
	    for (int i=0;i<100;i++) {
	    	outstr.writeShort(V100[i]);
	    }
	    byte []B = ((ByteArrayOutputStream) out).toByteArray();
	    return B;
	}
}