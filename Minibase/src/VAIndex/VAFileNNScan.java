package VAIndex;

import java.io.IOException;
import java.util.Arrays;

import index.IndexException;
import global.AttrType;
import global.PageId;
import global.RID;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;


public class VAFileNNScan {
	private Vector100Dtype target;
	private Heapfile hf;// data file
	private VAFile vaf;
	private int _count;
	private int _b;// va bit
	private RID[] NNrid = null;
	private VACandidate vac[] = null;
	
	public VAFileNNScan(Vector100Key key, 
			int count, String heapfilename, String vafilename, int vabit) 
					throws IndexException, VAException, 
					FieldNumberOutOfBoundException, IOException{
		//System.out.println("in VAFileNNScan");
		target = key.get_vector();
		_b = vabit;
		_count = count;
		NNrid = new RID[_count];
		vac = new VACandidate[_count];
		
	    try {
	        hf = new Heapfile(heapfilename);
	      }
	      catch (Exception e) {
	        throw new IndexException(e, "VAFileNNScan.java: Heapfile not created");
	      }
	    
	    
	    try {
    		vaf = new VAFile(vafilename,vabit); 
	      }
	      catch (Exception e) {
		throw new IndexException(e, "VAFileNNScan.java: VAFile exceptions caught from VAFile constructor");
	      }
	    
	    VA_SSA();
	}
	private void VA_SSA() throws VAException, FieldNumberOutOfBoundException, IOException{
		int largestdistance = initCandidate();
		RID rid1 = new RID();
		RID rid2 = new RID();
		Vector100Key vkey = null;
		KeyDataEntryVA keydata = null;
		Tuple temp = null;// for key
		Tuple temp2 = null;// for data
		Tuple t1 = new Tuple();
		// set tuple header for va key  t1
		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100Dkey);
		short[] attrSize = new short[1];
		attrSize[0] = (short)(_b*100/8+8);		
		try {
			t1.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int size = t1.size();
		t1 = new Tuple(size);
		try {
			t1.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// set tuple header for vector 100  t2
		Tuple t2 = new Tuple();
		attrType[0] = new AttrType(AttrType.attrVector100D);
		attrSize = new short[1];
		attrSize[0] = 0;// no use
		try {
			t2.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		size = t2.size();
		try {
			t2.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//open vafile index file
		VAFileScan vascan = null;
		try {
			vascan = new VAFileScan(vaf);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		// get all keys
		Vector100Dtype tmpVec;
		try {
			temp = vascan.getNext(rid1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			//System.out.println("in VANN scan  fldcnt="+ temp.noOfFlds());
//			System.out.println("in VANN scan  temp.size="+ temp.size());
			//System.out.println("in VANN scan  fldcnt="+ t1.noOfFlds());
			t1.tupleCopy(temp);
			

			try {
//				System.out.println("in NNScan "+Arrays.toString(t1.returnTupleByteArray()).length());		
//				System.out.println("in NNScan "+Arrays.toString(t1.returnTupleByteArray())  );//debug
				keydata = t1.get100DVectKeyFld((short)1);
				vkey = keydata.getKey();
				rid2 = keydata.getRid();
				System.out.println("in NNScan rid "+rid2.slotNo+" "+rid2.pageNo.pid);//debug
			} catch (Exception e) {

				e.printStackTrace();
			}
			vkey.setAllRegionNumber();
//			System.out.println("in VANN2 getLowerBoundDistance " 
//			+vkey.getLowerBoundDistance(this.target) );//debug
//			vkey.printAllRegionNumber();//debug

			if (vkey.getLowerBoundDistance(this.target) < largestdistance)
				// in this case, real data need to be fetched
				
				// if condition false, the key is filtered out
			{

				try{
					temp2 = hf.getRecord(rid2);
					t2.tupleCopy(temp2);
					
				}catch (Exception e) {

					e.printStackTrace();
				}
				tmpVec = t2.get100DVectFld(1);
				int realdistance = Vector100Dtype.distance(this.target, tmpVec);
//				System.out.println("in VANN2 realdistance "+realdistance);//debug
				largestdistance = this.Candidate(realdistance, rid2,tmpVec);
			}
			
			//get next key
			try{
				temp = vascan.getNext(rid1);
				
			}catch (Exception e) {

				e.printStackTrace();
			}
		
		}
		getResult();
	}
	public VACandidate[] getResult() {
//		System.out.println(" in getResult "+ _count +" "+vac.length);//debug
		for (int i=0;i<_count;i++){
//			System.out.println("in getResult2 "+vac[i].getDst());//debug
			vac[i].getVector().printVector();
			
		}
		return vac;
	}
	private int initCandidate(){
		int maxint = 0x7fffffff;// max int
		for (int i=0;i<this._count;i++){
			RID rid = new RID(new PageId(-1), -1);// invalid page, should be replaced later
			vac[i] = new VACandidate( maxint, rid);
			
		}
		return maxint;
	}
	private int Candidate (int realdst, RID rid, Vector100Dtype v){
//		System.out.println(" in Candidate**** "+realdst+" "+vac[0].getDst());//debug
//		System.out.println(" in Candidate**** "+realdst+" "+vac[1].getDst());
		if (realdst < vac[0].getDst())
		{
			vac[0] = new VACandidate(realdst, rid,v);
			sortCandidate();
		}
		return vac[0].getDst();
		
	}
	private void sortCandidate(){
		VACandidate tmpc = null;
		for (int i=0;i<this._count-1;i++){
			for (int j=i+1;j<this._count;j++){
				if (vac[i].getDst() < vac[j].getDst())
				{
					tmpc = vac[i];
					vac[i] = vac[j];
					vac[j] = tmpc;
				}
			}
		}
	}

}
class VACandidate {
	private int dst;
	private RID rid;
	private Vector100Dtype vector;
	
	public RID getRid() {
		return rid;
	}
	public VACandidate(int dst, RID rid){
		this.dst = dst;
		this.rid = rid;
	}
	public Vector100Dtype getVector() {
		return vector;
	}
	public void setVector(Vector100Dtype vector) {
		this.vector = vector;
	}
	public VACandidate(int dst, RID rid, Vector100Dtype vector){
		this.dst = dst;
		this.rid = rid;
		this.vector = vector;
	}
	public int getDst() {
		return dst;
	}
}
