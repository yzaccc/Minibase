package tests;

import global.AttrType;
import global.SystemDefs;
import global.TupleOrder;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.Projection;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import VAIndex.RSIndexScan;

public class Query2 extends TestDriver{
	public static Vector100Dtype TargetforSort;
	public static String IndexOption = "N";
	public static AttrType[] attrType;
	public static String RELNAME1;
	public static String RELNAME2;
	public static String QSNAME="/Users/akun1012/Desktop/Sort.txt";
	public static int NUMBUF;
	
	public Sort sortIndex(int QA, String T, ArrayList<Integer> sequenceOfNum)
	{
	System.out.print("In Sort\n");
	BufferedReader Targetbr = null;
	this.attrType = new AttrType[QA];
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
	phase2test.TargetforSort = new Vector100Dtype(Targetarray);// Target
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
	FldSpec[] projlist = new FldSpec[attrType.length];
	FileScan fscan = null;
	for (int i = 0; i < attrType.length; i++)
	{
		projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
	}
	try
	{
		fscan = new FileScan(RELNAME1, attrType, null, (short) attrType.length,
				attrType.length, projlist, null);
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
		//if (IsRange == true)
		sort = new Sort(attrType, (short) attrType.length, null, fscan, QA,
				order[0], Vector100Dtype.Max * 2, phase2test.numbuf,
				phase2test.TargetforSort, 0);
		//else
		//	sort = new Sort(attrType, (short) attrType.length, null, fscan, QA,
		//			order[0], Vector100Dtype.Max * 2, phase2test.numbuf,
		//			phase2test.TargetforSort, topk);
	} catch (SortException | IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return sort;
	}
	
	public boolean run(){
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
		
	    if(queryCommand !=null){
	    	String[] queryCommandSplit = queryCommand.split("\\(");
			String operationType = queryCommandSplit[0].trim();
			if(operationType.equals("Sort") || operationType == "Sort"){
				String parameterString = queryCommandSplit[1].trim().substring(0, queryCommandSplit[1].length()-1);
				String[] paraList = parameterString.split(",");
				int QA = Integer.parseInt(paraList[0].trim());
				String T = paraList[1].trim();
				ArrayList<Integer> sequenceOfNum = new ArrayList<Integer>();
				for(int i = 2; i<paraList.length; i++){
					sequenceOfNum.add(Integer.parseInt(paraList[i]));
				}
				
				Sort s = sortIndex(QA,T,sequenceOfNum);
				Tuple t=null;
				
					try {
						t = s.get_next();
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
				
				AttrType[] attr = new AttrType[4];
				attr[0] = new AttrType(2);
				attr[1] = new AttrType(5);
				attr[2] = new AttrType(2);
				attr[3] = new AttrType(5);
				try {
					t.setHdr((short)4, attr, null);
				} catch (InvalidTypeException | InvalidTupleSizeException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					t.get100DVectFld(2).printVector();
				} catch (FieldNumberOutOfBoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				};
			//Range---------------------
			}else if(operationType.equals("Range") || operationType == "Range"){
				String parameterString = queryCommandSplit[1].trim().substring(0, queryCommandSplit[1].length()-1);
				String[] paraList = parameterString.split(",");
				int QA = Integer.parseInt(paraList[0].trim());
				String T = paraList[1].trim();
				int D = Integer.parseInt(paraList[2].trim());
				int range = D;
				if(D < 0){
					System.out.println("Non negative integer D!!!!");
				}
				String I = paraList[3].trim();
				ArrayList<Integer> sequenceOfNum = new ArrayList<Integer>();
				for(int i = 4; i<paraList.length; i++){
					sequenceOfNum.add(Integer.parseInt(paraList[i]));
				}
				String[] Outputflds = (String[]) sequenceOfNum.toArray();
				
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
				this.TargetforSort = new Vector100Dtype(Targetarray);// Target
				// Vector

				try
				{
					Targetbr.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (IndexOption.equals("N"))
				{
					System.out.print("Using Sort\n");
					Sort sort = sortIndex(QA, T, sequenceOfNum);
					Tuple tmp = null;
					int currentdistance = 0;
					try
					{
						tmp = sort.get_next();
						int resultnum = 0;
						while (currentdistance >= 0 && tmp != null)
						{

							resultnum++;
							tmp.setHdr((short) attrType.length, attrType, null);
							currentdistance = Vector100Dtype
									.distance(tmp.get100DVectFld(QA),
											phase2test.TargetforSort);
							if (currentdistance >= range)
							{
								break;
							}
							System.out.print("\n\n\nResult Tuple " + resultnum
									+ ":\n{\n");
							AttrType outputflds[] = new AttrType[Outputflds.length];
							for (int i = 0; i < outputflds.length; i++)
							{
								outputflds[i] = attrType[Integer
										.parseInt(Outputflds[i]) - 1];
							}
							Tuple JTuple = new Tuple();
							try
							{
								JTuple.setHdr((short) outputflds.length,
										outputflds, null);
							} catch (InvalidTypeException
									| InvalidTupleSizeException e)
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							FldSpec[] projlist = new FldSpec[Outputflds.length];
							RelSpec rel = new RelSpec(RelSpec.outer);
							for (int i = 0; i < outputflds.length; i++)
							{
								projlist[i] = new FldSpec(rel,
										Integer.parseInt(Outputflds[i]));
							}
							Projection.Project(tmp, attrType, JTuple, projlist,
									outputflds.length);
							Tuple tt = new Tuple(JTuple.size());
							tt.tupleCopy(JTuple);
							tt.setHdr((short) outputflds.length, outputflds, null);
							for (int i = 0; i < outputflds.length; i++)
							{
								switch (outputflds[i].attrType)
									{
									case 1:
									System.out.print(tt.getIntFld(i + 1));
									System.out.print(",\n");
									break;
									case 2:
									System.out.print(tt.getFloFld(i + 1));
									System.out.print(",\n");
									break;
									case 5:
									System.out.print("[");
									tt.get100DVectFld(i + 1).printVector();
									System.out.print("],\n");
									break;
									}
							}
							System.out.print("}\n");
							tmp = sort.get_next();
						}
						while (tmp != null)
							tmp = sort.get_next();
						sort.close();
						SystemDefs.JavabaseBM.flushAllPages();
					} catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else if (IndexOption.equals("H"))
				{
					
				}else if (IndexOption.equals("B"))
				{
					
				}
				
				
				
				
			//DJOIN-------------	
			}else if(queryCommand.contains("DJOIN")){
				String parameterStringTmp = queryCommandSplit[2].trim().substring(0, queryCommandSplit[2].length()-1);
				String[] temp2 = parameterStringTmp.split("\\)");
				String[] rangeParaList = temp2[0].trim().split(",");
				String[] paraList = temp2[1].substring(1, temp2[1].length()).split(",");
				
				int QA1 = Integer.parseInt(rangeParaList[0].trim());
				String T1 = rangeParaList[1].trim();
				int D1 = Integer.parseInt(rangeParaList[2].trim());
				if(D1 < 0){
					System.out.println("Non negative integer D!!!!");
				}
				String I1 = rangeParaList[3].trim();
				ArrayList<Integer> DJOINinRangeSequenceOfNum = new ArrayList<Integer>();
				for(int i = 4; i<rangeParaList.length; i++){
					DJOINinRangeSequenceOfNum.add(Integer.parseInt(rangeParaList[i]));
				}
				
				
				int QA2 = Integer.parseInt(paraList[0].trim());
				int D2 = Integer.parseInt(paraList[1].trim());
				String I2 = paraList[2].trim();
				ArrayList<Integer> DJOINoutRangeSequenceOfNum = new ArrayList<Integer>();
				for(int i = 3; i<paraList.length; i++){
					DJOINoutRangeSequenceOfNum.add(Integer.parseInt(paraList[i]));
				}
			}
		}
		
		
		System.out.println("end");
		return status;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length == 4){
//			RELNAME1 = args[0];
//			RELNAME2 = args[1];
//			QSNAME = args[2];
//			NUMBUF = Integer.parseInt(args[3]);
		}
		
		Query query = new Query();
		query.run();
	}

}
