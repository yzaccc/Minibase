package VAIndex;

import java.io.IOException;
import java.util.ArrayList;

import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.PredEval;
import iterator.Projection;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.Vector100Dtype;

public class RSIndexScan {
	
	private Vector100Dtype target;
	private Heapfile hf;// data file
	private VAFile vaf;
	//private int _count;
	private int _b;// va bit
	//private VACandidate vac[] = null;
	private ArrayList<VACandidate> vac;
	private int nextidx = 0;
	private int _distance;
	
	// copy fields
	private int  _fldNum;  // field number for indexed field
	private int  _noInFlds;
	private AttrType[]    _types;
	private short[]       _s_sizes; // input tuple string size
	private Tuple         Jtuple;// result tuple
	private CondExpr[]    _selects;
	public FldSpec[]      perm_mat;
	private int           _noOutFlds;
	private Tuple         tuplein;// input tuple
	private int           t1_size;
	
	
	
	
	
	
	public RSIndexScan(IndexType index,
			String heapfilename, 
			String indName, 
			AttrType[] types, 
			short[] str_sizes, 
			int noInFlds, 
			int noOutFlds, 
			FldSpec[] outFlds,
			CondExpr[] selects,
			int fldNum,
			Vector100Dtype query, 
			int distance, int vabit) throws IndexException, IOException, VAException, FieldNumberOutOfBoundException{
		// ***************   copy1 begin  *************************//
	    _fldNum = fldNum;  
	    _noInFlds = noInFlds;
	    _types = types;  
	    _s_sizes = str_sizes;  
	    
	    
	    AttrType[] Jtypes = new AttrType[noOutFlds];
	    short[] ts_sizes;
	    Jtuple = new Tuple();
	    
	    
	    // setup  Jtuple  output tuple
	    try {
	        ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
	      }
	      catch (TupleUtilsException e) {
	        throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
	      }
	      catch (InvalidRelation e) {
	        throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
	      }
	    
	    _selects = selects;
	    perm_mat = outFlds;
	    _noOutFlds = noOutFlds;
	    
	    tuplein = new Tuple();    
	    try {
	    	tuplein.setHdr((short) noInFlds, types, str_sizes);
	    }
	    catch (Exception e) {
	      throw new IndexException(e, "IndexScan.java: Heapfile error");
	    }
	    
	    t1_size = tuplein.size();// input tuple size

	    
		// ***************   copy1 end  *************************//
	    
	    
	    // set again make sure it is correct
	    tuplein = new Tuple(t1_size);
	    try {
	    	tuplein.setHdr((short) noInFlds, types, str_sizes);
		    }
	    catch (Exception e) {
	      throw new IndexException(e, "IndexScan.java: Heapfile error");
	    }
	    
	    
	    
	    

	    
	    
		target = query;
		_b = vabit;
		_distance = distance;
		//vac = new VACandidate[_count];
		vac = new ArrayList<VACandidate>();
		
	    try {
	        hf = new Heapfile(heapfilename);
	      }
	      catch (Exception e) {
	        throw new IndexException(e, "VAFileNNScan.java: Heapfile not created");
	      }
	    
	    
	    try {
    		vaf = new VAFile(indName,vabit); 
	      }
	      catch (Exception e) {
		throw new IndexException(e, "VAFileNNScan.java: VAFile exceptions caught from VAFile constructor");
	      }
	    
	    VA_RS();

	    
	    
		
	}
	
	/**
	 * perform range scan, store result in vac
	 * @throws VAException
	 * @throws FieldNumberOutOfBoundException
	 * @throws IOException
	 */
	private void VA_RS() throws VAException, FieldNumberOutOfBoundException, IOException{

	    nextidx = 0;
//	    _count = 0;
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
		//Tuple t2 = new Tuple();






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
//				System.out.println("in NNScan rid "+rid2.slotNo+" "+rid2.pageNo.pid);//debug
			} catch (Exception e) {

				e.printStackTrace();
			}
			vkey.setAllRegionNumber();
//			System.out.println("in VANN2 getLowerBoundDistance " 
//			+vkey.getLowerBoundDistance(this.target) );//debug
//			vkey.printAllRegionNumber();//debug

			if (vkey.getLowerBoundDistance(this.target) <= this._distance)
				// in this case, real data need to be fetched
				
				// if condition false, the key is filtered out
			{

				try{
					temp2 = hf.getRecord(rid2);
					tuplein.tupleCopy(temp2);
					
				}catch (Exception e) {

					e.printStackTrace();
				}
				tmpVec = tuplein.get100DVectFld(_fldNum);// get indexed field
				int realdistance = Vector100Dtype.distance(this.target, tmpVec);
//				System.out.println("in VANN2 realdistance "+realdistance);//debug
				if (realdistance <= this._distance)
					// find qualified data
				{
					this.storeData(realdistance, rid2,tmpVec,tuplein);
				}
			}
			
			//get next key
			try{
				temp = vascan.getNext(rid1);
				
			}catch (Exception e) {

				e.printStackTrace();
			}
		
		}
//		getResult();//debug
		
	}
	
	/**
	 * Store result in vac
	 * @param realdst
	 * @param rid
	 * @param v
	 * @param tuple
	 */
	private void storeData (int realdst, RID rid, Vector100Dtype v, Tuple tuple){

		VACandidate tmp = new VACandidate(realdst, rid,v,tuple);
//		System.out.println("in Candidate size"+tuple.size());	
		vac.add(tmp);
//		this._count++;
		
	}
	
	public Tuple get_next() throws IndexException {
//		System.out.println("in NN get next nextidx = "+ nextidx);
		if (nextidx == this.vac.size())// no more 
			return null;
	   boolean eval;

	   Tuple nexttuple = vac.get(nextidx).getTuple();
	   tuplein.tupleCopy(nexttuple);
	   
	   nextidx++;
	   try {
		   TupleUtils.target = this.target;
		   eval = PredEval.Eval(_selects, tuplein, null, _types, null);
	   }
	   catch (Exception e) {
    	  throw new IndexException(e, "IndexScan.java: Heapfile error");
	   }
	      
      if (eval) {
		try {
//			System.out.println("call projection");
		  Projection.Project(tuplein, _types, Jtuple, perm_mat, _noOutFlds);
		}
		catch (Exception e) {
		  throw new IndexException(e, "IndexScan.java: Heapfile error");
		}

    		return Jtuple;
	   }
      return null;
	}

}
