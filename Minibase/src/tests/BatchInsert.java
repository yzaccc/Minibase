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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import VAIndex.VAException;
import VAIndex.VAFile;
import VAIndex.Vector100Key;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
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

class BatchInsertDriver extends TestDriver
{
	public BatchInsertDriver()
		{
		super("");
		}

	private Heapfile f = null;
	private short numColumns;
	private String[] brStrArray;
	private int[] columnsType;
	private AttrType[] attrArray;
	private String[] indexfilearray;
	private Tuple t = new Tuple();
	private ArrayList<VAFile> vaindexFileList = new ArrayList<VAFile>();
	private ArrayList<BTreeFile> BTreeFileList = new ArrayList<BTreeFile>();
	private ArrayList<String> VAindexFileColumnIDList = new ArrayList<String>();
	private ArrayList<String> BTreeindexFileColumnIDList = new ArrayList<String>();
	private ArrayList<Integer> VAindexFileBitNumList = new ArrayList<Integer>();
	private ArrayList<Integer> BTreeindexFileBitNUmList = new ArrayList<Integer>();

	@SuppressWarnings("resource")
	public boolean runTest(String updatefilename, String relname)
	{
	SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
	System.out.print("Open DB done.\n");
	boolean success = false;
	System.out.println(updatefilename);
	System.out.println(relname);
	PrintWriter specfile = null;
	boolean FileCreated = new File(dbpath + relname + ".spec").exists();
	if (!FileCreated)
	{

		try
		{
			specfile = new PrintWriter(dbpath + relname + ".spec");
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
	BufferedReader updatefileReader = null;
	// brStr is used to store on line read from br.
	String brStr = null;
	String[] brStrArray;
	try
	{
		updatefileReader = new BufferedReader(new FileReader(updatefilename));
	} catch (FileNotFoundException e)
	{
		e.printStackTrace();
	}
	// Read the first two lines
	// Store numcolumns and attrbuite
	try
	{
		brStr = updatefileReader.readLine();
		if (!FileCreated)
			specfile.println(brStr);
		numColumns = Short.parseShort(brStr.trim());
		brStr = updatefileReader.readLine();
		if (!FileCreated)
			{
			specfile.println(brStr);
			specfile.close();
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

	} catch (IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	// Initialize Tuple T
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

	// Read .indexspec file, get all index file name
	String indexfilestr = null;
	BufferedReader indexFileNameReader = null;
	boolean haveindex = true;
	try
	{
		indexFileNameReader = new BufferedReader(new FileReader(dbpath
				+ relname + ".indexspec"));
	} catch (FileNotFoundException e1)
	{
		haveindex = false;
	}

	if (haveindex)
	{
		ArrayList<String> indexFileNameList = new ArrayList<String>();
		try
		{
			indexfilestr = indexFileNameReader.readLine();
		} catch (IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int vaindexfilecount = 0;
		int btreeindexfilecount = 0;

		int bitnum = 0;
		String columnid = null;
		while (indexfilestr != null)
		{
			indexFileNameList.add(indexfilestr);
			try
			{

				if (indexfilestr.contains("vaindexfile"))
				{
					System.out.println(indexfilestr);
					String bitnumstr = indexfilestr.split("_")[3];
					bitnum =Integer.parseInt(bitnumstr); 
					VAindexFileBitNumList.add(bitnum);
					VAindexFileColumnIDList.add(indexfilestr.split("_")[1]);
					try
					{
						vaindexFileList.add(new VAFile(indexfilestr, bitnum));
					} catch (HFException | HFBufMgrException
							| HFDiskMgrException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					vaindexfilecount++;
				}
				else
				{
					System.out.println(indexfilestr);
					String bitnumstr = indexfilestr.split("_")[3];
					bitnum =Integer.parseInt(bitnumstr); 
					BTreeindexFileBitNUmList.add(bitnum);
					BTreeindexFileColumnIDList.add(indexfilestr.split("_")[1]);
					try
					{
						BTreeFileList.add(new BTreeFile(indexfilestr,
								AttrType.attrVector100Dkey, Vector100Key
										.getVAKeyLength(bitnum), 1));
					} catch (GetFileEntryException | ConstructPageException
							| AddFileEntryException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					btreeindexfilecount++;
				}
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try
			{
				indexfilestr = indexFileNameReader.readLine();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// Open those Index file;

	// Open the heap file to store tuple;
	try
	{
		f = new Heapfile(relname);
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	short[] vectorData = new short[100];
	Vector100Dtype vector = new Vector100Dtype((short) 0);
	RID rid = new RID();
	while (brStr != null)
	{
		for (int i = 0; i < numColumns; i++)
		{
			try
			{
				brStr = updatefileReader.readLine();
				if (brStr == null)
					break;
			} catch (IOException e)
			{
				e.printStackTrace();
			}
			switch (columnsType[i])
				{
				case 1:
				try
				{
					t.setIntFld(i + 1, Integer.parseInt(brStr));
				} catch (NumberFormatException | FieldNumberOutOfBoundException
						| IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				case 2:
				try
				{
					t.setFloFld(i + 1, Float.parseFloat(brStr.trim()));
				} catch (NumberFormatException | FieldNumberOutOfBoundException
						| IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				case 3:
				attrArray[i] = new AttrType(AttrType.attrString);
				break;
				case 4:
				brStrArray = brStr.split(" ");
				for (int i1 = 0; i1 < 100; i1++)
				{
					vectorData[i1] = Short.parseShort(brStrArray[i1]);
				}
				vector.setVectorValue(vectorData);
				try
				{
					t.set100DVectFld(i + 1, vector);
				} catch (FieldNumberOutOfBoundException | IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				default:
				System.out.print("Type not supported\n");
				break;
				}
		}
		try
		{
			rid = f.insertRecord(t.getTupleByteArray());

			if (haveindex)
			{
				Vector100Dtype vectorForIndex = null;
				Vector100Key key = null;
				for (int i = 0; i < vaindexFileList.size(); i++)
				{
					int column = Integer.parseInt(VAindexFileColumnIDList
							.get(i));
					vectorForIndex = t.get100DVectFld(column);
					key = new Vector100Key(vectorForIndex,
							VAindexFileBitNumList.get(i));
					vaindexFileList.get(i).insertKey(key, rid);
				}
				for (int i = 0; i < BTreeFileList.size(); i++)
				{
					int column = Integer.parseInt(BTreeindexFileColumnIDList
							.get(i));
					vectorForIndex = t.get100DVectFld(column);
					key = new Vector100Key(vectorForIndex,
							BTreeindexFileBitNUmList.get(i));
					try
					{
						BTreeFileList.get(i).insert(key, rid);
					} catch (KeyTooLongException | KeyNotMatchException
							| LeafInsertRecException | IndexInsertRecException
							| ConstructPageException | UnpinPageException
							| PinPageException | NodeNotMatchException
							| ConvertException | DeleteRecException
							| IndexSearchException | IteratorException
							| LeafDeleteException | InsertException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} catch (InvalidSlotNumberException | InvalidTupleSizeException
				| SpaceNotAvailableException | HFException | HFBufMgrException
				| HFDiskMgrException | IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FieldNumberOutOfBoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (VAException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	if (haveindex)
	{
		for (int i = 0; i < BTreeFileList.size(); i++)
		{
			try
			{
				BTreeFileList.get(i).close();
			} catch (PageUnpinnedException | InvalidFrameNumberException
					| HashEntryNotFoundException | ReplacerException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	// Scan scan = null;
	// try
	// {
	// scan = new Scan(f);
	// } catch (InvalidTupleSizeException | IOException e)
	// {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// RID rid1 = new RID();
	// Tuple tmp=null;
	// try
	// {
	// tmp = scan.getNext(rid1);
	//
	// } catch (InvalidTupleSizeException | IOException e)
	// {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// while(tmp!=null){
	// t.tupleCopy(tmp);
	// Vector100Dtype v1=null;
	// try
	// {
	// v1 = t.get100DVectFld(2);
	// } catch (FieldNumberOutOfBoundException | IOException e)
	// {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// v1.printVector();
	// try
	// {
	// tmp = scan.getNext(rid1);
	//
	// } catch (InvalidTupleSizeException | IOException e)
	// {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// success = true;
	// scan.closescan();
	try
	{
		SystemDefs.JavabaseBM.flushAllPages();
	} catch (HashOperationException | PageUnpinnedException
			| PagePinnedException | PageNotFoundException | BufMgrException
			| IOException e)
	{
		e.printStackTrace();
	}
	success = true;
	return success;
	}
}

public class BatchInsert
{
	public static void main(String argv[])
	{
	boolean insertStatus = false;
	BatchInsertDriver batchInsert = new BatchInsertDriver();
	insertStatus = batchInsert.runTest(argv[0], argv[1]);
	if (insertStatus == false)
	{
		System.out.print("Batch Insert Failed.\n");
	}
	else
	{
		System.out.print("Bathch Insert Success.\n");
	}
	}
}