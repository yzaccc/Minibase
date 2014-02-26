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
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;

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
		  IOException {
	  KeyDataEntryVA kdva = new KeyDataEntryVA(key, rid);
	  insertRecord(kdva.getBytesFromEntry());
  }
	
	
	

}
