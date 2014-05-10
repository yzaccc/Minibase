package tests;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

class DebugDriver extends TestDriver {
	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	Heapfile f = null;
	private Tuple t1 = new Tuple();
	private String[] brStrArray;

	public DebugDriver() {
		super("");
	}


	public void printFile(String relname) {
		SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF,
				"Clock");
		AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrReal);
		attrType[1] = new AttrType(AttrType.attrVector100D);
		attrType[2] = new AttrType(AttrType.attrReal);
		attrType[3] = new AttrType(AttrType.attrVector100D);
		
		try {
			t1.setHdr((short) 4, attrType, null);
			int size = t1.size();
			t1 = new Tuple(size);
			t1.setHdr((short) 4, attrType, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			f = new Heapfile(relname);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int cnt = 0;
//		System.out.println("after create");

		Scan scan = null;
		try {
			scan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		RID rid = new RID();
		Tuple tmp = null;
		try {
			tmp = scan.getNext(rid);
			
		} catch (InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}
		Vector100Dtype v1 = null;
		while (tmp != null) {
			cnt++;
			try {
				t1.tupleCopy(tmp);
				v1 = t1.get100DVectFld(2);
			} catch (FieldNumberOutOfBoundException | IOException e) {
				e.printStackTrace();
			}
			System.out.println("rid="+rid.pageNo.pid+" "+rid.slotNo);
			v1.printVector();
			try {
				tmp = scan.getNext(rid);
			} catch (InvalidTupleSizeException | IOException e) {
				e.printStackTrace();
			}
		}

		scan.closescan();
		System.out.println(" total " + cnt + " tuples");
	}

}

public class Debug {

	public static void main(String argv[]) {
//		boolean createStatus = false;
//		System.out.print("Batch debug .\n");
//		DebugDriver d = new DebugDriver();
//		 d.printFile("rel1");
		int[] a = {101,102,101,103,100,101,103,100,101,103,102,100,101,102,102,101,103,101,103,102,101,101,101,102,100,101,100,103,102,103,100,102,101,103,101,103,102,101,102,102,102,102,103,102,102,101,101,102,101,103,101,102,102,103,101,102,101,102,101,102,102,102,103,101,103,102,100,103,103,102,101,101,102,103,102,100,101,103,102,103,102,100,100,103,100,100,101,102,101,102,100,100,101,102,102,103,102,102,103,101};
		short b[] = new short[100];
		for(int i=0;i<100;i++){
			b[i] = (short)a[i];
		}
		
		int[] a1 = {100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100};
		short b1[] = new short[100];
		for(int i=0;i<100;i++){
			b1[i] = (short)a1[i];
		}
		Vector100Dtype v1 = new Vector100Dtype(b);
		Vector100Dtype v2 = new Vector100Dtype(b1);
		System.out.println(
		Vector100Dtype.distance(v1, v2));
	}
}