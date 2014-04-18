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
import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;

class ColumnIndexingDriver extends TestDriver{
	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	Heapfile f = null;
	private Tuple t = new Tuple();

	public ColumnIndexingDriver()
		{
		super("");
		}

	@SuppressWarnings("resource")
	public boolean runTest(String relName, String columnIdString, String indexType)
	{
	int columnId= Integer.parseInt(columnIdString);
	PCounter.setZero();
	PCounterw.setZero();
	PCounterPinPage.setZero();
	SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
	System.out.print("Column indexing Begin.\n");
	boolean success = false;
	// br is used to read in the data file.
	BufferedReader br = null;
	// brStr is used to store on line read from br.
	String brStr = null;
	String[] brStrArray;
	
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
	int bitnum = 4;
	boolean status = OK;
	BTreeFile btf = null;
	VAFile vaf = null;
	//Btree or Heapfile based vaindex
	if(indexType == "B" || indexType.equals("B")){
		
		//Create Btree file index
		try {
			btf = new BTreeFile("VA_BTreeIndex", AttrType.attrVector100Dkey,
					Vector100Key.getVAKeyLength(bitnum), 1/* delete */);
			System.out.println("BTreeIndex created successfully.\n");
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}else if(indexType == "H" || indexType.equals("H")){
		System.out.println("open a vafile");// debug
		try {
			vaf = new VAFile("vaindexfile"+columnIdString, 4);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		// insert into heapfile & va file at same time
		Vector100Key vkey = null;
	}
	/*
	 * Set Tuple header, ready to insert records into Heapfile.
	 */

	AttrType[] attrtype = new AttrType[1];
	attrtype[0] = new AttrType(AttrType.attrVector100Dkey);
	short[] keylength = new short[1];
	keylength[0] = 4;
	try
	{
		t.setHdr((short)1, attrtype, keylength);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short)1, attrtype, keylength);
	} catch (InvalidTypeException | InvalidTupleSizeException | IOException e)
	{
		e.printStackTrace();
	}
	short[] vectorData = new short[100];
	Vector100Dtype vector = new Vector100Dtype((short) 0);
	Scan scan = null;
	try
	{
		scan = new Scan(f);
	} catch (InvalidTupleSizeException | IOException e1)
	{
		e1.printStackTrace();
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
					//vafile[i].insertKey(key, rid1);
					btf.insert(key, rid);
				} catch (KeyTooLongException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (KeyNotMatchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (LeafInsertRecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IndexInsertRecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ConstructPageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnpinPageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (PinPageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NodeNotMatchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ConvertException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DeleteRecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IndexSearchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IteratorException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (LeafDeleteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InsertException e) {
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
	//createStatus = columnIndexing.runTest(argv[0], argv[1]);
	createStatus = columnIndexing.runTest(argv[0], argv[1],argv[2]);
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
