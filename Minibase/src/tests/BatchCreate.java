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
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;

class BatchCreateDriver extends TestDriver {
	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	Heapfile f = null;
	private Tuple t = new Tuple();
	private String[] brStrArray;

	public BatchCreateDriver() {
		super("");
	}

	@SuppressWarnings("resource")
	public boolean runTest(String dataFileName, String relName) {
		PCounter.setZero();
		PCounterw.setZero();
		PCounterPinPage.setZero();
		SystemDefs sysdef = new SystemDefs(dbpath, 3000, GlobalConst.NUMBUF,
				"Clock");
		System.out.print("Batch Create Begin.\n");
		boolean success = false;
		// br is used to read in the data file.
		BufferedReader br = null;
		// brStr is used to store on line read from br.
		String brStr = null;

		try {
			br = new BufferedReader(new FileReader(dataFileName));
			System.out.println("open "+dataFileName+" in batch create");
			System.out.println("relation name "+relName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		/*
		 * Specfile is used create a .spec file to store information about
		 * column number etc.
		 */
		PrintWriter specfile = null;
		try {
			specfile = new PrintWriter(dbpath + relName + ".spec");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// Read first line, stored and save it to spec file.
		try {
			brStr = br.readLine();
			numColumns = Short.parseShort(brStr.trim());
			specfile.print(brStr.trim());
			specfile.print("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		/*
		 * Read the second line of data file, which contains file type
		 */
		try {
			brStr = br.readLine();
			specfile.print(brStr.trim());

			brStrArray = brStr.split(" ");
			columnsType = new int[numColumns];
			attrArray = new AttrType[numColumns];
			for (int i = 0; i < numColumns; i++) {
				columnsType[i] = Integer.parseInt(brStrArray[i]);
			}
			for (int i = 0; i < numColumns; i++) {
				switch (columnsType[i]) {
				case 1:
					attrArray[i] = new AttrType(AttrType.attrInteger);
					break;
				case 2:
					attrArray[i] = new AttrType(AttrType.attrReal);
					break;
				case 3:
					attrArray[i] = new AttrType(AttrType.attrString);
					break;
				case 4:
					attrArray[i] = new AttrType(AttrType.attrVector100D);
					break;
				default:
					System.out.print("Type not supported\n");
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * Create a Heapfile based on the rel name;
		 */

		try {
			f = new Heapfile(relName);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM
				.getNumBuffers()) {
			System.err.println("*** The heap file has left pages pinned\n");
		}
		/*
		 * Set Tuple header, ready to insert records into Heapfile.
		 */

		try {
			t.setHdr(numColumns, attrArray, null);
			int size = t.size();
			t = new Tuple(size);
			t.setHdr(numColumns, attrArray, null);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}
		short[] vectorData = new short[100];
		Vector100Dtype vector = new Vector100Dtype((short) 0);
aaa:		while (brStr != null) {
			for (int i = 0; i < numColumns; i++) {
				try {
					brStr = br.readLine();
					if (brStr == null)
						break aaa;
				} catch (IOException e) {
					e.printStackTrace();
				}
				switch (columnsType[i]) {
				case 1:
					try {
						t.setIntFld(i + 1, Integer.parseInt(brStr));
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case 2:
					try {
						t.setFloFld(i + 1, Float.parseFloat(brStr.trim()));
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case 3:
					attrArray[i] = new AttrType(AttrType.attrString);
					break;
				case 4:
					brStrArray = brStr.split(" ");
					for (int i1 = 0; i1 < 100; i1++) {
						vectorData[i1] = Short.parseShort(brStrArray[i1]);
					}
					vector.setVectorValue(vectorData);
					try {
						t.set100DVectFld(i + 1, vector);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				default:
					System.out.print("Type not supported\n");
					break;
				}
			}
			try {
				f.insertRecord(t.getTupleByteArray());
//				System.out.println("insert in batch create");
			} catch (InvalidSlotNumberException | InvalidTupleSizeException
					| SpaceNotAvailableException | HFException
					| HFBufMgrException | HFDiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// debug
		// Scan scan = null;
		// try
		// {
		// scan = new Scan(f);
		// } catch (InvalidTupleSizeException | IOException e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// RID rid = new RID();
		// Tuple tmp = null;
		// try
		// {
		// tmp = scan.getNext(rid);
		// t.tupleCopy(tmp);
		// } catch (InvalidTupleSizeException | IOException e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// Vector100Dtype v1 = null;
		// try
		// {
		// v1 = t.get100DVectFld(2);
		// } catch (FieldNumberOutOfBoundException | IOException e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// v1.printVector();
		// scan.closescan();
		// debug
		success = true;
		specfile.close();

		if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM
				.getNumBuffers()) {
			System.err.println("*** Insertion left a page pinned\n");
		}
		try {
			SystemDefs.JavabaseBM.flushAllPages();
		} catch (HashOperationException | PageUnpinnedException
				| PagePinnedException | PageNotFoundException | BufMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.print("The number of write page is "
				+ PCounterPinPage.counter + "\n");
		
		return success;
	}

	public void printFile(String relname) {
		int cnt = 0;
		System.out.println("after create");

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
				t.tupleCopy(tmp);
				v1 = t.get100DVectFld(2);
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

public class BatchCreate {

	public static void main(String argv[]) {
		boolean createStatus = false;
		BatchCreateDriver batchInsert = new BatchCreateDriver();
		createStatus = batchInsert.runTest(argv[0], argv[1]);
//		 batchInsert.printFile(argv[1]);
		if (createStatus == false) {
			System.out.print("Batch Create Failed.\n");
		} else {
			System.out.print("Batch Create Success.\n");
		}
	}
}
