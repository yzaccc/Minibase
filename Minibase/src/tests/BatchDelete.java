package tests;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
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
import heap.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import VAIndex.VAFile;
import VAIndex.Vector100Key;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IntegerKey;
import bufmgr.BufMgrException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

class BatchDeleteDriver extends TestDriver {

	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	private Tuple t = new Tuple();

	private ArrayList<Integer> VAindexFileBitNumList = new ArrayList<Integer>();
	private ArrayList<String> VAindexFileColumnIDList = new ArrayList<String>();
	private ArrayList<VAFile> vaindexFileList = new ArrayList<VAFile>();

	private ArrayList<Integer> BTreeindexFileBitNUmList = new ArrayList<Integer>();
	private ArrayList<String> BTreeindexFileColumnIDList = new ArrayList<String>();
	private ArrayList<BTreeFile> BTreeFileList = new ArrayList<BTreeFile>();

	// delete key
	private int gi1;
	private float gf1;
	private Vector100Dtype gvector = new Vector100Dtype((short) 0);
	private ArrayList<RID> ridlist = new ArrayList<RID>();

	private Heapfile f = null;

	public BatchDeleteDriver() {
		super("");
	}

	public boolean runTest(String updatefilename, String relname) {
		SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF,
				"Clock");
		System.out.print("Open DB done.\n");
		boolean success = false;
		System.out.println(updatefilename);
		System.out.println(relname);
		PrintWriter specfile = null;
		boolean FileCreated = new File(dbpath + relname + ".spec").exists();
		if (!FileCreated) {

			try {
				specfile = new PrintWriter(dbpath + relname + ".spec");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		BufferedReader updatefileReader = null;
		// brStr is used to store one line read from br.
		String brStr = null;
		String[] brStrArray;
		try {
			updatefileReader = new BufferedReader(
					new FileReader(updatefilename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// Read the first two lines
		// Store numcolumns and attrbuite
		try {
			brStr = updatefileReader.readLine();
			if (!FileCreated)
				specfile.println(brStr);
			numColumns = Short.parseShort(brStr.trim());
			brStr = updatefileReader.readLine();
			if (!FileCreated) {
				specfile.println(brStr);
				specfile.close();
			}

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

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// Initialize Tuple T
		try {
			t.setHdr(numColumns, attrArray, null);
			int size = t.size();
			t = new Tuple(size);
			t.setHdr(numColumns, attrArray, null);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}

		// Read .indexspec file, get all index file name
		String indexfilestr = null;
		BufferedReader indexFileNameReader = null;
		boolean haveindex = true;
		try {
			indexFileNameReader = new BufferedReader(new FileReader(dbpath
					+ relname + ".indexspec"));
		} catch (FileNotFoundException e1) {
			haveindex = false;
		}

		if (haveindex) {
			ArrayList<String> indexFileNameList = new ArrayList<String>();
			try {
				indexfilestr = indexFileNameReader.readLine();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			int vaindexfilecount = 0;
			int btreeindexfilecount = 0;

			int bitnum = 0;
			String columnid = null;
			while (indexfilestr != null) {
				indexFileNameList.add(indexfilestr);
				try {

					if (indexfilestr.contains("vaindexfile")) {
						System.out.println(indexfilestr);
						String bitnumstr = indexfilestr.split("_")[3];
						bitnum = Integer.parseInt(bitnumstr);
						VAindexFileBitNumList.add(bitnum);
						VAindexFileColumnIDList.add(indexfilestr.split("_")[1]);
						try {
							vaindexFileList
									.add(new VAFile(indexfilestr, bitnum));
						} catch (HFException | HFBufMgrException
								| HFDiskMgrException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						vaindexfilecount++;
					} else {
						System.out.println(indexfilestr);
						String bitnumstr = indexfilestr.split("_")[3];
						bitnum = Integer.parseInt(bitnumstr);
						BTreeindexFileBitNUmList.add(bitnum);
						BTreeindexFileColumnIDList
								.add(indexfilestr.split("_")[1]);
						try {
							BTreeFileList.add(new BTreeFile(indexfilestr,
									AttrType.attrVector100Dkey, Vector100Key
											.getVAKeyLength(bitnum), 1));
						} catch (GetFileEntryException | ConstructPageException
								| AddFileEntryException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						btreeindexfilecount++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					indexfilestr = indexFileNameReader.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		// Open those Index file;

		// Open the heap file to store tuple;
		try {
			f = new Heapfile(relname);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		RID rid = null;

aaa:		while (brStr != null) {
			// read in data

			try {
				brStr = updatefileReader.readLine();
				if (brStr == null || brStr.equals(""))
					break aaa;
			} catch (IOException e) {
				e.printStackTrace();
			}
			brStrArray = brStr.split(" ");
			int colnum = Integer.parseInt(brStrArray[0]);
			// colnum--;// columnsType start from 0
			switch (columnsType[colnum - 1]) {
			case 1:
				gi1 = Integer.parseInt(brStrArray[1]);
				IntegerKey ikey = new IntegerKey(gi1);
				break;
			case 2:
				gf1 = new Float(Float.parseFloat(brStrArray[1]));
				break;
			// case 3:
			// // attrArray[i] = new AttrType(AttrType.attrString);
			// break;
			case 4:
				short[] vectorData = new short[100];
				for (int i2 = 0; i2 < 100; i2++) {
					vectorData[i2] = Short.parseShort(brStrArray[i2 + 1]);
				}
				gvector.setVectorValue(vectorData);
				break;
			default:
				System.out.print("Type not supported\n");
				break;
			}

			this.findRID(colnum);
			RID rid1 = null;
			for (int jj = 0; jj < ridlist.size(); jj++) {
				rid1 = ridlist.get(jj);

				if (haveindex) {

					// delete from va file
					for (int i = 0; i < vaindexFileList.size(); i++) {
						try {
							vaindexFileList.get(i).deleteKey(rid1);
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
					// delete from btree file
					for (int i = 0; i < BTreeFileList.size(); i++) {
						int bcolumn = Integer
								.parseInt(BTreeindexFileColumnIDList.get(i));
						Tuple tmptuple = new Tuple();
						try {
							tmptuple = f.getRecord(rid1);
							t.tupleCopy(tmptuple);
						} catch (Exception e2) {
							e2.printStackTrace();
						}
						if (columnsType[bcolumn - 1] == 4) {
							// for vector key

							Vector100Key vkey = null;
							Vector100Dtype vectorForIndex = null;
							try {
								vectorForIndex = t.get100DVectFld(bcolumn);
								vkey = new Vector100Key(vectorForIndex,
										BTreeindexFileBitNUmList.get(i));
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							try {
								BTreeFileList.get(i).Delete(vkey, rid1);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						// no intger key
						// else if (columnsType[bcolumn - 1] == 1) {
						// int i1 = -1;
						// try {
						// i1 = t.getIntFld(bcolumn);
						// } catch (Exception e) {
						// e.printStackTrace();
						// }
						// IntegerKey ikey = new IntegerKey(i1);
						// try {
						// BTreeFileList.get(i).insert(ikey, rid1);
						// } catch (Exception e) {
						// e.printStackTrace();
						// }
						//
						// }
					}
					// delete from heap file

				}
				try {
					f.deleteRecord(rid1);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		if (haveindex) {
			for (int i = 0; i < BTreeFileList.size(); i++) {
				try {
					BTreeFileList.get(i).close();
				} catch (PageUnpinnedException | InvalidFrameNumberException
						| HashEntryNotFoundException | ReplacerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		try {
			System.out.println("before flush all ");
			SystemDefs.JavabaseBM.flushAllPages();
		} catch (HashOperationException | PageUnpinnedException
				| PagePinnedException | PageNotFoundException | BufMgrException
				| IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	public void findRID(int colnum) {
		RID rid0 = new RID(new PageId(-1), -1);
		Scan scan = null;
		Tuple temp = null;
		Tuple t2 = new Tuple();
		// set header
		try {
			t2.setHdr(numColumns, attrArray, null);
			int size = t2.size();
			t2 = new Tuple(size);
			t2.setHdr(numColumns, attrArray, null);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}
		// open scan
		try {
			scan = new Scan(f);
		} catch (InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}

		RID rid1 = new RID();
		Tuple tmp = null;
		// get all tuple from heap file
		try {
			tmp = scan.getNext(rid1);

		} catch (InvalidTupleSizeException | IOException e) {
			e.printStackTrace();
		}
		while (tmp != null) {
			t2.tupleCopy(tmp);
			// compare two tuple
			if (this.compare(t2, colnum) == true) {
				System.out.println("find rid " + rid1.pageNo.pid + " "
						+ rid1.slotNo);
				ridlist.add(rid1);
			}
			// get next tuple
			try {
				tmp = scan.getNext(rid1);

			} catch (InvalidTupleSizeException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		scan.closescan();

	}

	public boolean compare(Tuple t2, int colnum) {
		Vector100Dtype v1 = null;

		int i1 = -1;
		float f1 = -1;

		String s1 = null;

		switch (columnsType[colnum - 1]) {
		case 1:
			try {
				i1 = t2.getIntFld(colnum);
			} catch (FieldNumberOutOfBoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (i1 != gi1)
				return false;
			break;
		case 2:
			try {
				f1 = t2.getFloFld(colnum);

			} catch (FieldNumberOutOfBoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (f1 != gf1)
				return false;
			break;
		// case 3:
		// // no string
		// break;
		case 4:
			try {
				v1 = t2.get100DVectFld(colnum);
			} catch (FieldNumberOutOfBoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (Vector100Dtype.distance(v1, gvector) != 0)
				return false;
			break;
		default:
			System.out.print("Type not supported\n");
			break;
		}

		return true;

	}

	public void printFile(String relname) {
		int cnt = 0;
		System.out.println("after delete");

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
			System.out.println("rid=" + rid.pageNo.pid + " " + rid.slotNo);
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

public class BatchDelete {

	public static void main(String argv[]) {
		boolean deleteStatus = false;
		BatchDeleteDriver batchDelete = new BatchDeleteDriver();
		deleteStatus = batchDelete.runTest(argv[0], argv[1]);
		batchDelete.printFile(argv[1]);
		if (deleteStatus == false) {
			System.out.print("Batch Delete Failed.\n");
		} else {
			System.out.print("Bathch Delete Success.\n");
		}
	}

}