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
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class VAFile extends Heapfile {
	public static final short LOWERBOUND = -10000;
	public static final short UPPERBOUND = 10000;
	public static final short MAXRANGE = 20000;
	private int _b; // bits per dimension

	public int get_b() {
		return _b;
	}

	/**
	 * 
	 * @param filename
	 *            file name
	 * @param b
	 *            bits per dimension
	 * @throws IOException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws HFException
	 */
	public VAFile(String filename, int b) throws HFException,
			HFBufMgrException, HFDiskMgrException, IOException {
		super(filename);
		_b = b;
		// if file exist, I can't guarantee b is the same when open file again
		// but it can be checked later when get record from the file
		// record.length*8/100 = b
	}

	public void insertKey(Vector100Key key, RID rid)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException,
			HFDiskMgrException, IOException, FieldNumberOutOfBoundException {
		KeyDataEntryVA kdva = new KeyDataEntryVA(key, rid);
		// System.out.println("in VAFile "+kdva.getBytesFromEntry().length);//debug
		Tuple t1 = new Tuple();
		// set tuple header for va key t1
		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100Dkey);
		short[] attrSize = new short[1];
		attrSize[0] = (short) (Vector100Key.getVAKeyLength(this._b) + 8);
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
		t1.set100DVectkeyridFld(1, kdva);
		// System.out.println("key size "+t1.size());
		insertRecord(t1.getTupleByteArray());
	}

	public void deleteKey(RID rid1) {
		Scan scan = null;
		RID rid2 = new RID();// va file rid
		RID rid3 = new RID();// va file rid
		Tuple temp = null;// for key
		Tuple t1 = new Tuple();
		KeyDataEntryVA keydata = null;
		// set tuple header for va key t1
		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100Dkey);
		short[] attrSize = new short[1];
		attrSize[0] = (short) (Vector100Key.getVAKeyLength(this._b) + 8);
		try {
			t1.setHdr((short) 1, attrType, attrSize);
			int size = t1.size();
			t1 = new Tuple(size);
			t1.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// open scan find data file rid
		try {
			scan = this.openScan();
		} catch (InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}
		try {
			temp = scan.getNext(rid2);
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (temp != null) {
			t1.tupleCopy(temp);
			try {

				keydata = t1.get100DVectKeyFld((short) 1);
				rid3 = keydata.getRid();

				// System.out.println("in NNScan9 rid "+rid2.slotNo+" "+rid2.pageNo.pid);//debug
			} catch (Exception e) {

				e.printStackTrace();
			}
			if (rid3.equals(rid1)) {
				try {
					this.deleteRecord(rid2);
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			}

			try {
				temp = scan.getNext(rid2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	

		scan.closescan();
		try
		{
			System.out.println("flush all pages in VAFile");
			SystemDefs.JavabaseBM.flushAllPages();
		} catch (HashOperationException | PageUnpinnedException
				| PagePinnedException | PageNotFoundException | BufMgrException
				| IOException e)
		{
			e.printStackTrace();
		}

	}

}