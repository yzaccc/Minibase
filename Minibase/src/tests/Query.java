package tests;

import global.AttrType;
import global.GlobalConst;
import global.IndexType;
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
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.PredEval;
import iterator.Projection;
import iterator.RelSpec;
import iterator.UnknownKeyTypeException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import btree.KeyClass;
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
			for (int i = 2; i < paraList.length; i++)
			{
				sequenceOfNum.add(Integer.parseInt(paraList[i]));
			}
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
						switch(attrArray[i].attrType){
						case 1:
						try
						{
							System.out.print("\t"+t.getIntFld(i + 1));
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
							System.out.print("\t"+t.getFloFld(i + 1));
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
							t.get100DVectFld(i + 1).printVector();
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
				String _indexname = "VABTreeIndex"+ RELNAME1 + "_" + QA + "_"
						+ "B" + "_" + bitnumstr;
				short[] attrSize = new short[1];
				attrSize[0] = 200;
				FldSpec[] projlist = new FldSpec[1];
				projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
				hf = new Heapfile(RELNAME1);
				IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index),
						RELNAME1,_indexname, attrArray, attrSize, 1, 1, projlist,
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
					if(indexApproximateDistance < D)
					{
						Tuple tmp = hf.getRecord(rid);
						t.tupleCopy(tmp);
						realDistance = Vector100Dtype.distance(t.get100DVectFld(QA),
								TargetVector);
						if (realDistance <= D)
						{
							System.out.println("Real distance is "+realDistance);
							System.out.print("innerVector is");
							t.get100DVectFld(QA).printVector();
							System.out.print("\n");
							System.out.print("outerVector is");
							TargetVector.printVector();
							System.out.print("\n");
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
			String[] paraList = temp2[1].substring(1, temp2[1].length()).split(
					",");

			int QA1 = Integer.parseInt(rangeParaList[0].trim());
			String T1 = rangeParaList[1].trim();
			int D1 = Integer.parseInt(rangeParaList[2].trim());
			if (D1 < 0)
			{
				System.out.println("Non negative integer D!!!!");
			}
			String I1 = rangeParaList[3].trim();
			ArrayList<Integer> DJOINinRangeSequenceOfNum = new ArrayList<Integer>();
			for (int i = 4; i < rangeParaList.length; i++)
			{
				DJOINinRangeSequenceOfNum.add(Integer
						.parseInt(rangeParaList[i]));
			}

			int QA2 = Integer.parseInt(paraList[0].trim());
			int D2 = Integer.parseInt(paraList[1].trim());
			String I2 = paraList[2].trim();
			ArrayList<Integer> DJOINoutRangeSequenceOfNum = new ArrayList<Integer>();
			for (int i = 3; i < paraList.length; i++)
			{
				DJOINoutRangeSequenceOfNum.add(Integer.parseInt(paraList[i]));
			}
		}
	}

	System.out.println("end");
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
