package VAIndex;

import btree.BTreeFile;
import index.IndexException;
import global.AttrType;
import global.PageId;
import global.RID;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.Scan;
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
			int count, String heapfilename, String vafilename, int vabit) throws IndexException{
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
	private void VA_SSA(){
		RID rid = new RID();
		Vector100Key vkey = null;
		Tuple temp = null;
		Tuple t = new Tuple();
		// set tuple header
		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100Dkey);
		short[] attrSize = new short[1];
		attrSize[0] = (short)(_b*100/8);
		int size = t.size();
		t = new Tuple(size);
		try {
			t.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//open a vascan
		VAFileScan vascan = null;
		try {
			vascan = new VAFileScan(hf);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		// get all keys
		
		try {
			temp = vascan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			t.tupleCopy(temp);

			try {
				vkey = t.get100DVectKeyFld((short)1);
			} catch (Exception e) {

				e.printStackTrace();
			}
			vkey.setAllRegionNumber();
		
		}
	}
	private void initCandidate(){
		int maxint = 0x7fffffff;// max int
		for (int i=0;i<this._count;i++){
			RID rid = new RID(new PageId(-1), -1);
			vac[i] = new VACandidate( maxint, rid);
			
		}
	}
	private int Candidate (int d, RID rid){
		if (d<vac[0].getDst())
		{
			vac[0] = new VACandidate(d, rid);
			sortCandidate();
		}
		
	}
	private void sortCandidate(){
		RId tmprid;
		for 
	}

}
class VACandidate {
	private int dst;
	public int getDst() {
		return dst;
	}
	private RID rid;
	public VACandidate(int dst, RID rid){
		this.dst = dst;
		this.rid = rid;
	}
}
