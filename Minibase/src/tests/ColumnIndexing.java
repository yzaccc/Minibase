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
import java.io.*;

import VAIndex.VAException;
import VAIndex.VAFile;
import VAIndex.Vector100Key;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.BufMgrException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;

class ColumnIndexingDriver extends TestDriver
{
	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	private String[] brStrArray;
	private Scan scan;
	Heapfile f = null;
	private Tuple t = new Tuple();

	public ColumnIndexingDriver()
		{
		super("");
		}

	@SuppressWarnings("resource")
	public boolean runTest(String relName, String columnIdString,
			String indexType, String bitnumstr)
	{
	int columnId = Integer.parseInt(columnIdString);
	PCounter.setZero();
	PCounterw.setZero();
	PCounterPinPage.setZero();
	SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
	System.out.print("Open DB done.\n");
	boolean success = false;
	// br is used to read in the data file.
	BufferedReader br = null;
	// brStr is used to store on line read from br.
	String brStr = null;
	String[] brStrArray;
	
	/*
	 * Create a index spec file
	 */
	PrintWriter specfile = null;
	String filename = dbpath + relName +".indexspec";
	try
	{
		specfile = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
	} catch (FileNotFoundException e)
	{
		e.printStackTrace();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	/*
	 * Create a Heapfile based on the rel name;
	 */

	
	try
	{
		f = new Heapfile(relName);
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	try
	{
		scan = new Scan(f);
	} catch (InvalidTupleSizeException | IOException e2)
	{
		// TODO Auto-generated catch block
		e2.printStackTrace();
	}
	
	System.out.println("unpinned BUFFER is:"+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
	System.out.println("Number of Buffer is:"+SystemDefs.JavabaseBM.getNumBuffers());
	if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()) {
		System.err.println("*** The heap-file scan has not pinned the first page\n");
	}
	int bitnum = Integer.parseInt(bitnumstr);
	BTreeFile btf = null;
	VAFile vaf = null;
	// Btree or Heapfile based vaindex
	if (indexType == "B" || indexType.equals("B"))
	{

		// Create Btree file index
		try
		{
			String btreefilename = "VA_BTreeIndex"+columnIdString+indexType+bitnum;
			btf = new BTreeFile(btreefilename, AttrType.attrVector100Dkey,
					Vector100Key.getVAKeyLength(bitnum), 1/* delete */);
			System.out.println("BTreeIndex created successfully.\n");
			specfile.println(btreefilename);
		} catch (Exception e)
		{
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] attrtype = new AttrType[1];
		attrtype[0] = new AttrType(AttrType.attrVector100Dkey);
		short[] keylength = new short[1];
		keylength[0] = 4;
		try
		{
			br = new BufferedReader(new FileReader(dbpath + relName + ".spec"));
			numColumns = Short.parseShort(br.readLine());
		} catch (FileNotFoundException e2)
		{
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (NumberFormatException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try
		{
			brStr = br.readLine();
		} catch (IOException e2)
		{
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		brStrArray = brStr.split(" ");
		columnsType = new int[numColumns];
		attrArray = new AttrType[numColumns];
		for (int i = 0; i < numColumns; i++)
		{
			columnsType[i] = Integer.parseInt(brStrArray[i]);
		}
		for (int i = 0; i < numColumns; i++)
		{
			switch (columnsType[i])
				{
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
		try
		{
			t.setHdr(numColumns, attrArray, null);
			int size = t.size();
			t = new Tuple(size);
			t.setHdr(numColumns, attrArray, null);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e)
		{
			e.printStackTrace();
		}
		RID rid = new RID();
		Vector100Key key = null;
		Vector100Dtype[] vectorforIndex = new Vector100Dtype[1];
		try
		{
			Tuple tmp = scan.getNext(rid);
			while (tmp != null)
			{
				t.tupleCopy(tmp);
				for (int i = 0; i < 1; i++)
				{
					vectorforIndex[i] = t.get100DVectFld(columnId);
					try
					{
						key = new Vector100Key(vectorforIndex[i], bitnum);
					} catch (VAException e1)
					{
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					try
					{
						// vafile[i].insertKey(key, rid1);
						btf.insert(key, rid);
					} catch (KeyTooLongException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KeyNotMatchException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (LeafInsertRecException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IndexInsertRecException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ConstructPageException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnpinPageException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (PinPageException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NodeNotMatchException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ConvertException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (DeleteRecException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IndexSearchException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IteratorException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (LeafDeleteException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InsertException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				tmp = scan.getNext(rid);
			}

		} catch (InvalidTupleSizeException | IOException e1)
		{
			e1.printStackTrace();
		} catch (FieldNumberOutOfBoundException e)
		{
			e.printStackTrace();
		}
		try
		{
			btf.close();
		} catch (PageUnpinnedException | InvalidFrameNumberException
				| HashEntryNotFoundException | ReplacerException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	else if (indexType == "H" || indexType.equals("H"))
	{
		System.out.println("open a vafile");// debug
		try
		{
			String heapindexfilename = "vaindexfile"+columnIdString+indexType+bitnum;
			specfile.println(heapindexfilename);
			vaf = new VAFile(heapindexfilename, bitnum);
		} catch (Exception e)
		{
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		/*
		 * We create a Tuple srcTuple to store tupe get from scan
		 */
		Tuple srcTuple = new Tuple();
		/* Open the spec file and get information about the Tuple, e.g
		 * Tuple number and Tuple attribute 
		 */
		try
		{
			br = new BufferedReader(new FileReader(dbpath + relName + ".spec"));
			numColumns = Short.parseShort(br.readLine());
		} catch (FileNotFoundException e2)
		{
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (NumberFormatException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try
		{
			brStr = br.readLine();
		} catch (IOException e2)
		{
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		brStrArray = brStr.split(" ");
		columnsType = new int[numColumns];
		attrArray = new AttrType[numColumns];
		for (int i = 0; i < numColumns; i++)
		{
			columnsType[i] = Integer.parseInt(brStrArray[i]);
		}
		for (int i = 0; i < numColumns; i++)
		{
			switch (columnsType[i])
				{
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
		try
		{
			srcTuple.setHdr(numColumns, attrArray, null);
			int size = srcTuple.size();
			srcTuple = new Tuple(size);
			srcTuple.setHdr(numColumns, attrArray, null);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e)
		{
			e.printStackTrace();
		}
		
		//Open a Scan, for each tuple we get, we create a key tuple
		//and write it into the indexfile
		RID rid = new RID();
		Vector100Key key = null;
		Vector100Dtype vectorforIndex = new Vector100Dtype((short)0);
		try
		{
			Tuple tmp = scan.getNext(rid);
			while (tmp != null)
			{
				srcTuple.tupleCopy(tmp);
				for (int i = 0; i < 1; i++)
				{
					vectorforIndex = srcTuple.get100DVectFld(columnId);
					try
					{
						key = new Vector100Key(vectorforIndex, bitnum);
					} catch (VAException e1)
					{
						e1.printStackTrace();
					}
					try
					{
						vaf.insertKey(key, rid);
					} catch (InvalidSlotNumberException
							| SpaceNotAvailableException | HFException
							| HFBufMgrException | HFDiskMgrException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				tmp = scan.getNext(rid);
			}

		} catch (InvalidTupleSizeException | IOException e1)
		{
			e1.printStackTrace();
		} catch (FieldNumberOutOfBoundException e)
		{
			e.printStackTrace();
		}

	}
	/*
	 * Set Tuple header, ready to insert records into Heapfile.
	 */
	scan.closescan();
	specfile.close();
	try
	{
		SystemDefs.JavabaseBM.flushAllPages();
	} catch (HashOperationException | PageUnpinnedException
			| PagePinnedException | PageNotFoundException | BufMgrException
			| IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	success = true;
	return success;
	}
}

public class ColumnIndexing
{

	public static void main(String argv[])
	{
	boolean createStatus = false;
	ColumnIndexingDriver columnIndexing = new ColumnIndexingDriver();
	createStatus = columnIndexing.runTest(argv[0], argv[1], argv[2], argv[3]);
	if (createStatus == false)
	{
		System.out.print("Column indexing Failed.\n");
	}
	else
	{
		System.out.print("Column indexing Success.\n");
	}
	}
}