package VAIndex;

import java.io.IOException;

import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class VAFile extends Heapfile
{
	public static final short LOWERBOUND = -10000;
	public static final short UPPERBOUND = 10000;
	public static final short MAXRANGE = 20000;
	private int _b; // bits per dimension
	
	
	public  int get_b() {
		return _b;
	}


	/**
	 * 
	 * @param filename file name
	 * @param b bits per dimension
	 * @throws IOException 
	 * @throws HFDiskMgrException 
	 * @throws HFBufMgrException 
	 * @throws HFException 
	 */
	public VAFile (String filename, int b) throws HFException, HFBufMgrException, HFDiskMgrException, IOException{
		super(filename);
		_b = b;
		// if file exist, I can't guarantee b is the same when open file again
		// but it can be checked later when get record from the file
		// record.length*8/100 = b
		}
	
  public void insertKey(Vector100Key key, RID rid) 
		  throws InvalidSlotNumberException, 
		  InvalidTupleSizeException, 
		  SpaceNotAvailableException, 
		  HFException, 
		  HFBufMgrException, 
		  HFDiskMgrException, 
		  IOException, FieldNumberOutOfBoundException {
	  KeyDataEntryVA kdva = new KeyDataEntryVA(key, rid);
	  //System.out.println("in VAFile "+kdva.getBytesFromEntry().length);//debug
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
		t1.set100DVectkeyFld(1, kdva);
//		System.out.println("key size "+t1.size());
		insertRecord(t1.getTupleByteArray());
  }
	
	
	

}
