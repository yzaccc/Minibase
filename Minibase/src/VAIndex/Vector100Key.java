package VAIndex;

import java.util.Arrays;

import global.Vector100Dtype;
import btree.KeyClass;

public class Vector100Key extends KeyClass{
	
	public int getDataLength() {
		return dataLength;
	}

	private int _b;// bits per dimension
	private int dataLength;// length of key in bytes
	private double regionsize;// size of one region in a dimension, same for all dimension
	private int totalregionnum;// number of region in a dimension
	private byte []data;
	private Vector100Dtype _vector;
	
	
	public Vector100Dtype get_vector() {
		return _vector;
	}
	public Vector100Key(int b) throws VAException{
		_b = b;
		if (b%2 == 0)
		{ 
			dataLength = b*100/8;// convert to byte
			data = new byte [dataLength];
			totalregionnum = 1<<b;
			regionsize = VAFile.MAXRANGE / (double)totalregionnum; //divide one dimention into 2^b parts
		}
		else
			throw new VAException(null, "bit number should be even");
	}

	public Vector100Key(Vector100Dtype v, int b) throws VAException{
		_b = b;
		_vector = v;
		
		if (b%2 == 0)
		{ 
			dataLength = b*100/8;// convert to byte
			data = new byte [dataLength];
			totalregionnum = 1<<b;
			regionsize = VAFile.MAXRANGE / (double)totalregionnum; //divide one dimention into 2^b parts
		}
		else
			throw new VAException(null, "bit number should be even");
		
		short [] vecvalue = v.getVectorValue();
		int regionnum;
		int bitmask;
		StringBuffer binarydata = new StringBuffer();
		for (int i=0;i<100;i++){
			if (vecvalue[i] > VAFile.UPPERBOUND)
				throw new VAException(null, "vector value larger than upper bound");
			else if (vecvalue[i] < VAFile.LOWERBOUND)
				throw new VAException(null, "vector value lower than lower bound");
			regionnum = (int)((vecvalue[i] - VAFile.LOWERBOUND)/regionsize);
			if (regionnum == totalregionnum)//last region should be 2^b-1, change 2^b to 2^b-1
				regionnum -= 1;
			String binaryregion = Integer.toBinaryString(regionnum);// binary number of region
			StringBuffer tmpsb = new StringBuffer();
			if (binaryregion.length() < _b){// padding zero
				
				for (int j=binaryregion.length();j<_b;j++){
					tmpsb.append("0");
				}
			}
			tmpsb.append(binaryregion);
			binarydata.append(tmpsb);		
		}
		for (int i=0;i<dataLength;i++){
			String tmpstr = new String(binarydata.substring(i*8, i*8+8));
			byte numberbyte = (byte) Integer.parseInt(tmpstr,2);
			byte []numberbytearray = new byte [1];
			numberbytearray[0] = numberbyte;
			System.arraycopy (numberbytearray, 0, data, i, 1);
		}
		//System.out.println("in Vector100Key 2");//debug
		//System.out.println(Arrays.toString(data));//debug
		//System.out.println(binarydata);//debug
		
	}
	
	public byte [] returnKeyByteArray(){
		return data;
	}
	public void setDataBytes(byte [] data, int position){
		System.arraycopy(data, position, this.data, 0, this.dataLength);
	}
 
}
