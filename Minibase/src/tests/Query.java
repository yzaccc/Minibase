package tests;

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.RangeScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.INLJoins;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEval;
import iterator.Projection;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;
import btree.KeyClass;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import VAIndex.RSIndexScan;
import VAIndex.VAException;
import VAIndex.Vector100Key;

public class Query extends TestDriver
{
	public static String RELNAME1;
	public static String RELNAME2;
	public static String QSNAME;
	public static int NUMBUF;

	private short numColumns;
	private int[] columnsType;
	private AttrType[] attrArray;
	private String[] brStrArray;
	private Vector100Dtype TargetVector;
	private Tuple t;
	private int TupleCount;
	private Heapfile hf;
	public Query()
		{
		super("");
		}
	public Sort sortIndex(int QA, String T, ArrayList<Integer> sequenceOfNum)
	{
	System.out.print("In Sort\n");
	Vector100Dtype TargetforSort;
	BufferedReader Targetbr = null;
	BufferedReader relInfoReader=null;

	PCounter.setZero();
	PCounterw.setZero();
	PCounterPinPage.setZero();
	try
	{
		relInfoReader = new BufferedReader(new FileReader(
				dbpath + RELNAME1 + ".spec"));
	} catch (FileNotFoundException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	try
	{
		numColumns = Short.parseShort(relInfoReader.readLine());
	} catch (NumberFormatException | IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	String brStr = null;
	try
	{
		brStr = relInfoReader.readLine();
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
		Targetbr = new BufferedReader(new FileReader(T));
	} catch (FileNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	String[] TargetStr = new String[100];
	try
	{
		TargetStr = Targetbr.readLine().trim().split(" ");
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	short[] Targetarray = new short[100];
	for (int i = 0; i < 100; i++)
	{
		Targetarray[i] = Short.parseShort(TargetStr[i]);
	}
	TargetforSort = new Vector100Dtype(Targetarray);// Target
	// Vector

	try
	{
		Targetbr.close();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	// Create ProjectList, Or output flds
	FldSpec[] projlist = new FldSpec[attrArray.length];
	FileScan fscan = null;
	for (int i = 0; i < attrArray.length; i++)
	{
		projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
	}
	try
	{
		fscan = new FileScan(RELNAME1, attrArray, null, (short) attrArray.length,
				attrArray.length, projlist, null);
	} catch (FileScanException | TupleUtilsException | InvalidRelation
			| IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	TupleOrder[] order = new TupleOrder[2];
	order[0] = new TupleOrder(TupleOrder.Ascending);
	order[1] = new TupleOrder(TupleOrder.Descending);
	Sort sort = null;
	try
	{
		sort = new Sort(attrArray, (short) attrArray.length, null, fscan, QA,
				order[0], Vector100Dtype.Max * 2, NUMBUF/2,
				TargetforSort, 0);
	} catch (SortException | IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return sort;
	}
	@SuppressWarnings("resource")
	public boolean run() throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
	SystemDefs sysdef = new SystemDefs(dbpath, 0, NUMBUF, "Clock");
	boolean status = true;
	BufferedReader Querybr = null;
	String queryCommand = null;
	try
	{
		Querybr = new BufferedReader(new FileReader(QSNAME));
	} catch (FileNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	try
	{
		queryCommand = Querybr.readLine().trim();
	} catch (IOException e)
	{
		e.printStackTrace();
	}

	if (queryCommand != null)
	{
		String[] queryCommandSplit = queryCommand.split("\\(");
		String operationType = queryCommandSplit[0].trim();
		if (operationType.equals("Sort") || operationType == "Sort")
		{
			String parameterString = queryCommandSplit[1].trim().substring(0,
					queryCommandSplit[1].length() - 1);
			String[] paraList = parameterString.split(",");
			int QA = Integer.parseInt(paraList[0].trim());
			String T = paraList[1].trim();
			ArrayList<Integer> sequenceOfNum = new ArrayList<Integer>();
			
			BufferedReader relInfoReader = new BufferedReader(new FileReader(
					dbpath + RELNAME1 + ".spec"));
			numColumns = Short.parseShort(relInfoReader.readLine());
			String brStr = null;
			try
			{
				brStr = relInfoReader.readLine();
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
			t = new Tuple();
			t.setHdr(numColumns, attrArray, null);
			
			for (int i = 2; i < paraList.length; i++)
			{
				sequenceOfNum.add(Integer.parseInt(paraList[i]));
			}
			Sort s = sortIndex(QA,T,sequenceOfNum);
			
			Tuple tmp=null;
			
				try {
					tmp = s.get_next();
				} catch (SortException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (UnknowAttrType e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (LowMemException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (JoinsException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			TupleCount = 0; 
			while(tmp!=null){
				TupleCount++;
				System.out.print("Tuple" + TupleCount+":\n{");
				t.tupleCopy(tmp);
				
				for(int i=0;i<sequenceOfNum.size();i++)
				{
					switch(attrArray[sequenceOfNum.get(i)-1].attrType){
					case 1:
					try
					{
						System.out.print("\t"+t.getIntFld(sequenceOfNum.get(i)));
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.print(",\n");
					break;
					case 2:
					try
					{
						System.out.print("\t"+t.getFloFld(sequenceOfNum.get(i)));
						System.out.print(",\n");
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e1)
					{
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					break;
					case 5:
					System.out.print("\t");
					try
					{
						t.get100DVectFld(sequenceOfNum.get(i)).printVector();
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
					}
				}
				System.out.println("}");
				tmp = s.get_next();
			}
			
		
		
			s.close();
			SystemDefs.JavabaseBM.flushAllPages();
		}
		else if (operationType.equals("Range") || operationType == "Range")
		{
			String parameterString = queryCommandSplit[1].trim().substring(0,
					queryCommandSplit[1].length() - 1);
			String[] paraList = parameterString.split(",");
			int QA = Integer.parseInt(paraList[0].trim());
			String T = paraList[1].trim();
			String bitnumstr = paraList[2].trim();
			int bitnum = Integer.parseInt(bitnumstr);
			
			int D = Integer.parseInt(paraList[3].trim());
			String I = paraList[4].trim();
			if (D < 0)
			{
				System.out.println("Non negative integer D!!!!");
			}
			
			ArrayList<Integer> sequenceOfNum = new ArrayList<Integer>();
			for (int i = 5; i < paraList.length; i++)
			{
				sequenceOfNum.add(Integer.parseInt(paraList[i]));
			}

			// Open the relation spec file and get
			// information about Column Numer etc
			BufferedReader relInfoReader = new BufferedReader(new FileReader(
					dbpath + RELNAME1 + ".spec"));
			numColumns = Short.parseShort(relInfoReader.readLine());
			String brStr = null;
			try
			{
				brStr = relInfoReader.readLine();
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
			t = new Tuple();
			t.setHdr(numColumns, attrArray, null);
			BufferedReader Targetbr = null;
			try
			{
				Targetbr = new BufferedReader(new FileReader(T));
			} catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String[] TargetStr = new String[100];
			try
			{
				TargetStr = Targetbr.readLine().trim().split(" ");
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			short[] Targetarray = new short[100];
			for (int i = 0; i < 100; i++)
			{
				Targetarray[i] = Short.parseShort(TargetStr[i]);
			}
			TargetVector = new Vector100Dtype(Targetarray);// Target Vector

			try
			{
				Targetbr.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// If indexType is H
			if (I.equals("H"))
			{
				String indexName = "vaindexfile" + RELNAME1 + "_" + QA + "_"
						+ "H" + "_" + bitnumstr;
				if(indexName.equals("vaindexfilerel1_2_H_16"))
						System.out.println("Indexfile name is correct");
				short[] strsize = null;
				CondExpr[] selects = null;
				FldSpec[] projlist = new FldSpec[sequenceOfNum.size()];
				RelSpec rel = new RelSpec(RelSpec.outer);
				for (int i = 0; i < sequenceOfNum.size(); i++)
				{
					projlist[i] = new FldSpec(rel, sequenceOfNum.get(i));
				}

				RSIndexScan rscan = new RSIndexScan(new IndexType(
						IndexType.VAIndex), RELNAME1, indexName, attrArray,
						strsize, numColumns, sequenceOfNum.size(), projlist,
						selects, QA, TargetVector, D, bitnum);
				Tuple tmp = rscan.get_next();
				TupleCount = 0;
				while(tmp !=null)
				{
					t.tupleCopy(tmp);
					System.out.print("Tuple" + TupleCount+":\n{");
					for(int i=0;i<sequenceOfNum.size();i++)
					{
						switch(attrArray[sequenceOfNum.get(i)-1].attrType){
						case 1:
						try
						{
							System.out.print("\t"+t.getIntFld(sequenceOfNum.get(i)));
						} catch (NumberFormatException
								| FieldNumberOutOfBoundException
								| IOException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						System.out.print(",\n");
						break;
						case 2:
						try
						{
							System.out.print("\t"+t.getFloFld(sequenceOfNum.get(i)));
							System.out.print(",\n");
						} catch (NumberFormatException
								| FieldNumberOutOfBoundException
								| IOException e1)
						{
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						break;
						case 5:
						System.out.print("\t");
						try
						{
							t.get100DVectFld(sequenceOfNum.get(i)).printVector();
						} catch (NumberFormatException
								| FieldNumberOutOfBoundException
								| IOException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						break;
						}
					}
					System.out.println("}");
					tmp = rscan.get_next();
				}
				
			}
			//
			else if (I.equals("B"))
			{
				//VABTreeIndexrel1_2_B_16
				TupleCount = 0;
				String _indexname = "VABTreeIndex"+ RELNAME1 + "_" + QA + "_"
						+ "B" + "_" + bitnumstr;
				short[] attrSize = new short[1];
				attrSize[0] = 200;
				FldSpec[] projlist = new FldSpec[1];
				projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
				hf = new Heapfile(RELNAME1);
				AttrType[] attrIndexType = new AttrType[1];
				attrIndexType[0] = new AttrType(AttrType.attrVector100D);
				IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index),
						RELNAME1,_indexname, attrIndexType, attrSize, 1, 1, projlist,
						null, 1, true);
				RID rid = new RID();
				KeyClass key1 = null;
				Vector100Key vkey2 = null;
				int indexApproximateDistance = 0;
				int realDistance = 0;
				while ((key1 = iscan.get_nextKey(rid)) != null)
				{
					if (key1 instanceof Vector100Key)
					{
						vkey2 = (Vector100Key) key1;
					}
					indexApproximateDistance = vkey2
							.getLowerBoundDistance(TargetVector);
					if(indexApproximateDistance <= D)
					{
						Tuple tmp = hf.getRecord(rid);
						t.tupleCopy(tmp);
						realDistance = Vector100Dtype.distance(t.get100DVectFld(QA),
								TargetVector);
						if (realDistance <= D)
						{
							System.out.print("Tuple" + TupleCount+":\n{");
							TupleCount ++;
							for(int i=0;i<sequenceOfNum.size();i++)
							{
								switch(attrArray[sequenceOfNum.get(i)-1].attrType){
								case 1:
								try
								{
									System.out.print("\t"+t.getIntFld(sequenceOfNum.get(i)));
								} catch (NumberFormatException
										| FieldNumberOutOfBoundException
										| IOException e)
								{
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								System.out.print(",\n");
								break;
								case 2:
								try
								{
									System.out.print("\t"+t.getFloFld(sequenceOfNum.get(i)));
									System.out.print(",\n");
								} catch (NumberFormatException
										| FieldNumberOutOfBoundException
										| IOException e1)
								{
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								break;
								case 5:
								System.out.print("\t");
								try
								{
									t.get100DVectFld(sequenceOfNum.get(i)).printVector();
								} catch (NumberFormatException
										| FieldNumberOutOfBoundException
										| IOException e)
								{
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								break;
								}
							}
							System.out.println("}");
						}
					}
				}
			}
		}
		else if (queryCommand.contains("DJOIN"))
		{
			String parameterStringTmp = queryCommandSplit[2].trim().substring(
					0, queryCommandSplit[2].length() - 1);
			String[] temp2 = parameterStringTmp.split("\\)");
			String[] rangeParaList = temp2[0].trim().split(",");
			String[] paraList = temp2[1].substring(1, temp2[1].length()).split(",");

			int QA1 = Integer.parseInt(rangeParaList[0].trim());
			String T1 = rangeParaList[1].trim();
			int D1 = Integer.parseInt(rangeParaList[2].trim());
			if (D1 < 0)
			{
				System.out.println("Non negative integer D!!!!");
			}
			String I1 = rangeParaList[3].trim();
			String BitStrIndex1 = rangeParaList[4].trim();
			int bitNumIndex1 = Integer.parseInt(BitStrIndex1);
			ArrayList<Integer> DJOINoutRangeSequenceOfNum = new ArrayList<Integer>();
			for (int i = 5; i < rangeParaList.length; i++)
			{
				DJOINoutRangeSequenceOfNum.add(Integer
						.parseInt(rangeParaList[i]));
			}
			int QA2 = Integer.parseInt(paraList[0].trim());
			int D2 = Integer.parseInt(paraList[1].trim());
			String I2 = paraList[2].trim();
			ArrayList<Integer> DJOINinRangeSequenceOfNum = new ArrayList<Integer>();
			String BitStrIndex2 = rangeParaList[3].trim();
			int bitNumIndex2 = Integer.parseInt(BitStrIndex1);
			for (int i = 4; i < paraList.length; i++)
			{
				DJOINinRangeSequenceOfNum.add(Integer.parseInt(paraList[i]));
			}
			String indexName = null;
			if(I1.equals("H"))
			{
				indexName = "vaindexfile" + RELNAME1 + "_" + QA1 + "_"
						+ "H" + "_" + bitNumIndex1;
			}
			else if(I1.equals("B")){
				indexName = "VABTreeIndex"+ RELNAME1 + "_" + QA1 + "_"
						+ "B" + "_" + bitNumIndex1;	
			}
			
			
			BufferedReader Targetbr = null;
			try
			{
				Targetbr = new BufferedReader(new FileReader(T1));
			} catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String[] TargetStr = new String[100];
			try
			{
				TargetStr = Targetbr.readLine().trim().split(" ");
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			short[] Targetarray = new short[100];
			for (int i = 0; i < 100; i++)
			{
				Targetarray[i] = Short.parseShort(TargetStr[i]);
			}
			TargetVector = new Vector100Dtype(Targetarray);// Target Vector
			Targetbr.close();
			BufferedReader relInfoReader = new BufferedReader(new FileReader(
					dbpath + RELNAME1 + ".spec"));
			numColumns = Short.parseShort(relInfoReader.readLine());
			String brStr = null;
			try
			{
				brStr = relInfoReader.readLine();
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
			
			
			BufferedReader relInfoReader2 = new BufferedReader(new FileReader(
					dbpath + RELNAME2 + ".spec"));
			int numColumns2 = Short.parseShort(relInfoReader2.readLine());
			String brStr2 = null;
			try
			{
				brStr2 = relInfoReader2.readLine();
			} catch (IOException e2)
			{
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			String [] brStrArray2 = brStr2.split(" ");
			int[] columnsType2 = new int[numColumns2];
			AttrType [] attrArray2 = new AttrType[numColumns2];
			for (int i = 0; i < numColumns2; i++)
			{
				columnsType2[i] = Integer.parseInt(brStrArray2[i]);
			}
			for (int i = 0; i < numColumns2; i++)
			{
				switch (columnsType2[i])
					{
					case 1:
					attrArray2[i] = new AttrType(AttrType.attrInteger);
					break;
					case 2:
					attrArray2[i] = new AttrType(AttrType.attrReal);
					break;
					case 3:
					attrArray2[i] = new AttrType(AttrType.attrString);
					break;
					case 4:
					attrArray2[i] = new AttrType(AttrType.attrVector100D);
					break;
					default:
					System.out.print("Type not supported\n");
					break;
					}
			}
			//Create a rscan
			RangeScan rscan = new RangeScan(indexName, QA1, I1,
					RELNAME1, attrArray, D1,
					TargetVector, bitNumIndex1);
			String indexName2 = null;
			IndexType indextype = null;
			if(I2.equals("H"))
			{
				indexName2 = "vaindexfile" + RELNAME2 + "_" + QA2 + "_"
						+ "H" + "_" + bitNumIndex2;
				indextype = new IndexType(3);
			}
			else if(I2.equals("B")){
				indexName2 = "VABTreeIndex"+ RELNAME2 + "_" + QA2 + "_"
						+ "B" + "_" + bitNumIndex2;
				indextype = new IndexType(1);
			}
			short t1_str_sizes[]=null;
			short t2_str_sizes[]=null;
			
			//Create expression
			CondExpr[] expr = new CondExpr[3];
			expr[0] = new CondExpr();
			expr[0].op = new AttrOperator(AttrOperator.aopLE);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
			expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);
			expr[0].distance = D2;
			expr[0].next = null;
			expr[1] = null;
			CondExpr[] expr1 = null;
			
			//create projlist
			int JoinTupleColumnNum = attrArray.length + attrArray2.length;
			FldSpec[] projlist = new FldSpec[JoinTupleColumnNum];
			for (int i = 0; i < attrArray.length; i++)
			{
				projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			for (int i = attrArray.length; i < JoinTupleColumnNum; i++)
			{
				projlist[i] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1-attrArray.length);
			}
			
			INLJoins inlj = new INLJoins(attrArray, attrArray.length, t1_str_sizes,
					attrArray2, attrArray2.length, t2_str_sizes, NUMBUF,
					rscan, RELNAME2, indextype,
					indexName2, expr1,
					expr, projlist, 5);
			Tuple t=new Tuple();
			t = inlj.get_next();
			
			int RangeOuterSize = DJOINoutRangeSequenceOfNum.size();
			int RangeinnerSize = DJOINinRangeSequenceOfNum.size();
			int ResultTupleSize = RangeOuterSize+RangeinnerSize;
			FldSpec[] Resultprojlist = new FldSpec[ResultTupleSize];
			for (int i = 0; i < RangeOuterSize ; i++)
			{
				Resultprojlist[i] = new FldSpec(new RelSpec(RelSpec.outer), DJOINoutRangeSequenceOfNum.get(i));
			}
			for (int i = RangeOuterSize; i < ResultTupleSize ; i++)
			{
				Resultprojlist[i] = new FldSpec(new RelSpec(RelSpec.outer), attrArray.length+DJOINinRangeSequenceOfNum.get(i-RangeOuterSize));
			}
			AttrType [] ResultAttr = new AttrType[ResultTupleSize];
			int columnId=0;
			AttrType tmpAttr = null;
			for(int i=0;i<RangeOuterSize;i++){
				columnId = DJOINoutRangeSequenceOfNum.get(i);
				tmpAttr = attrArray[columnId-1];
				ResultAttr[i] = new AttrType(tmpAttr.attrType);
			}
			for(int i=RangeOuterSize;i<ResultTupleSize;i++)
			{
				columnId = DJOINinRangeSequenceOfNum.get(i-RangeOuterSize);
				tmpAttr = attrArray2[columnId-1];
				ResultAttr[i] = new AttrType(tmpAttr.attrType);
			}
			
			Tuple JTuple = new Tuple();
			JTuple.setHdr((short)ResultAttr.length, ResultAttr, null);
			int tSize = attrArray.length+attrArray2.length;
			AttrType [] AttrArrayAfterNLJ = new AttrType[tSize]; 
			for(int i=0;i<attrArray.length;i++)
			{
				AttrArrayAfterNLJ[i] = new AttrType(attrArray[i].attrType);
			}
			for(int i=attrArray.length;i<tSize;i++)
			{
				AttrArrayAfterNLJ[i] = new AttrType(attrArray2[i-attrArray.length].attrType);
			}
			TupleCount = 0;
			while(t!=null)
			{
				Projection.Project(t, AttrArrayAfterNLJ, JTuple, Resultprojlist, Resultprojlist.length);
					System.out.print("Tuple" + TupleCount+":\n{");
					TupleCount ++;
				for(int i=0;i<Resultprojlist.length;i++)
				{
					switch(ResultAttr[i].attrType){
					case 1:
					try
					{
						System.out.print("\t"+JTuple.getIntFld(i + 1));
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.print(",\n");
					break;
					case 2:
					try
					{
						System.out.print("\t"+JTuple.getFloFld(i + 1));
						System.out.print(",\n");
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e1)
					{
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					break;
					case 5:
					System.out.print("\t");
					try
					{
						JTuple.get100DVectFld(i + 1).printVector();
					} catch (NumberFormatException
							| FieldNumberOutOfBoundException
							| IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
					}
				}
				System.out.println("}");
				t = inlj.get_next();
				
			}
			try
			{
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (HashOperationException | PageUnpinnedException
					| PagePinnedException | PageNotFoundException | BufMgrException
					| IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	System.out.print("The number of pin page is "
			+ PCounterPinPage.counter + "\n");
	System.out.print("The number of Read page is " + PCounter.counter
			+ "\n");
	System.out.print("The number of write page in DB is "
			+ PCounterw.counter + "\n");
	return status;
	}

	public static void main(String[] args)
	{
	// TODO Auto-generated method stub
	if (args.length == 4)
	{
		RELNAME1 = args[0];
		RELNAME2 = args[1];
		QSNAME = args[2];
		NUMBUF = Integer.parseInt(args[3]);
	}

	Query query = new Query();
	try
	{
		query.run();
	} catch (IndexException | VAException | FieldNumberOutOfBoundException
			| IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTypeException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTupleSizeException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnknownIndexTypeException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidSlotNumberException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFDiskMgrException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFBufMgrException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

}
