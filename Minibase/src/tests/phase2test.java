package tests;

import index.IndexException;
import iterator.CondExpr;
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
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;
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
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import VAIndex.NNIndexScan;
import VAIndex.RSIndexScan;
import VAIndex.VAException;
import VAIndex.VAFile;
import VAIndex.Vector100Key;

class Phase2Driver extends TestDriver implements GlobalConst
{
	public Phase2Driver()
		{
		super("phase2test");
		}

	public static String DATAFILENAME = "/home/jinxuanw/demo_data/test_data.txt";
	public static String DBNAME = "test1.in";
	public static String QSNAME = "/home/jinxuanw/demo_data/nquery1.txt";
	public static String IndexOption = "N";
	public static AttrType[] attrType;
	public static int k = 4;// number of bits input
	public static int numtuple = 369;// input
	public Tuple t = new Tuple();
	public int vectorfld[];
	public int topk;
	public int cnt = 0;// number of lines of input
	Heapfile f = null;
	private static int Q;
	private static int QA;
	public static Vector100Dtype TargetforSort;
	public static Vector100Dtype TargetforNN;
	public static Vector100Dtype TargetforRange;

	public Sort sortIndex(String[] ArgumentList, String[] Outputflds,
			boolean IsRange)
	{
	QA = Integer.parseInt(ArgumentList[0]);
	Q = Integer.parseInt(ArgumentList[0]);
	String T = ArgumentList[1];
	topk = Integer.parseInt(ArgumentList[2]);
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
	FldSpec[] projlist = new FldSpec[attrType.length];
	FileScan fscan = null;
	for (int i = 0; i < attrType.length; i++)
	{
		projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
	}
	try
	{
		fscan = new FileScan(DBNAME, attrType, null, (short) attrType.length,
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
		sort = new Sort(attrType, (short) attrType.length, null, fscan, QA,
				order[0], Vector100Dtype.Max * 2, 30, TargetforSort, topk);
	} catch (SortException | IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return sort;
	}

	public NNIndexScan nnInexScan(String[] ArgumentList, String[] Outputflds)
	{
	QA = Integer.parseInt(ArgumentList[0]);
	String T = ArgumentList[1];
	int topk = Integer.parseInt(ArgumentList[2]);
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
	TargetforNN = new Vector100Dtype(Targetarray);// Target Vector
												  // done
	try
	{
		Targetbr.close();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	// Create ProjectList, Or output flds
	FldSpec[] projlist = new FldSpec[Outputflds.length];
	RelSpec rel = new RelSpec(RelSpec.outer);
	for (int i = 0; i < Outputflds.length; i++)
	{
		projlist[i] = new FldSpec(rel, Integer.parseInt(Outputflds[i]));
	}

	short[] strsize = null;
	CondExpr[] selects = null;
	NNIndexScan nnscan = null;
	int index = 0;
	for (int i = 0; i < vectorfld.length; i++)
	{
		if (vectorfld[i] == QA - 1)
		{
			index = i;
			break;
		}

	}

	try
	{
		nnscan = new NNIndexScan(new IndexType(IndexType.VAIndex), DBNAME,
				DBNAME + "_" + index, attrType, strsize, 4, Outputflds.length,
				projlist, selects, QA, TargetforNN, topk, k);

	} catch (Exception e)
	{
		e.printStackTrace();
	}
	return nnscan;
	}

	public RSIndexScan rangescan(String[] ArgumentList, String[] Outputflds)
	{
	Q = Integer.parseInt(ArgumentList[0]); // Query fldNum
	String T = ArgumentList[1];// String stored target file path
	int range = Integer.parseInt(ArgumentList[2]);
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
	TargetforRange = new Vector100Dtype(Targetarray);// Target Vector

	try
	{
		Targetbr.close();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	// Create ProjectList, Or output flds
	FldSpec[] projlist = new FldSpec[Outputflds.length];
	RelSpec rel = new RelSpec(RelSpec.outer);
	for (int i = 0; i < Outputflds.length; i++)
	{
		projlist[i] = new FldSpec(rel, Integer.parseInt(Outputflds[i]));
	}

	// Create RangeIndex Scan
	RSIndexScan rscan = null;
	short[] strsize = null;
	CondExpr[] selects = null;
	int index = 0;
	for (int i = 0; i < vectorfld.length; i++)
	{
		if (vectorfld[i] == Q - 1)
		{
			index = i;
			break;
		}

	}

	try
	{
		rscan = new RSIndexScan(new IndexType(IndexType.VAIndex), DBNAME,
				DBNAME + "_" + index, attrType, strsize, attrType.length,
				Outputflds.length, projlist, selects, Q, TargetforRange, range,
				k);
	} catch (IndexException | VAException | FieldNumberOutOfBoundException
			| IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return rscan;
	}

	public boolean runTests()
	{
	System.out.println("\n" + "Running " + testName() + " tests...." + "\n");
	SystemDefs sysdef = new SystemDefs(dbpath, 300, NUMBUF, "Clock");

	// Kill anything that might be hanging around
	String newdbpath;
	String newlogpath;
	String remove_logcmd;
	String remove_dbcmd;
	String remove_cmd = "/bin/rm -rf";

	newdbpath = dbpath;
	newlogpath = logpath;

	remove_logcmd = remove_cmd + logpath;
	remove_dbcmd = remove_cmd + dbpath;

	try
	{
		Runtime.getRuntime().exec(remove_logcmd);
		Runtime.getRuntime().exec(remove_dbcmd);
	} catch (IOException e)
	{
		System.err.println("" + e);
	}

	boolean _pass = runAllTests();

	try
	{
		Runtime.getRuntime().exec(remove_logcmd);
		Runtime.getRuntime().exec(remove_dbcmd);
	} catch (IOException e)
	{
		System.err.println("" + e);
	}

	System.out.println("\n" + "..." + testName() + " tests ");
	System.out.println(_pass == OK ? "completely successfully" : "failed");
	System.out.println(".\n\n");

	return _pass;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test1()
	{
	System.out
			.println("----------------- begin test1-------------------------");
	// PCounter.initialize();

	PCounter.setZero();
	PCounterw.setZero();
	PCounterPinPage.setZero();
	int num_fld;
	BufferedReader br = null;
	try
	{
		br = new BufferedReader(new FileReader(DATAFILENAME)); // Create buffer
															   // to read
															   // DATAFILENAME
	} catch (FileNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	// read num of fields
	String line = null;
	try
	{
		line = br.readLine(); // Read first line
	} catch (IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	num_fld = Integer.parseInt(line.trim()); // Set number of fields

	// read array type of fields
	try
	{
		line = br.readLine(); // Read Second Line, which contains numbers
							  // specify fields
	} catch (IOException e1)
	{
		e1.printStackTrace();
	}

	// Assign the second line of DATAFILE into fld_array
	String fld_str[] = line.split(" ");
	short[] fld_array = new short[fld_str.length];
	for (int i = 0; i < fld_str.length; i++)
	{
		fld_array[i] = Short.parseShort(fld_str[i]);
	}

	try
	{
		f = new Heapfile(DBNAME); // This should be the DBNAME
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

	// Read and stored
	String data_str[] = new String[num_fld];

	attrType = new AttrType[num_fld];
	int vectorfldnum = 0;
	for (int i = 0; i < num_fld; i++)
	{
		switch (fld_array[i])
			{
			case 1:
			attrType[i] = new AttrType(AttrType.attrInteger);
			break;
			case 2:
			attrType[i] = new AttrType(AttrType.attrReal);
			break;
			case 3:
			// currently Sample data don't have string, so ignore it.
			break;
			case 4:
			attrType[i] = new AttrType(AttrType.attrVector100D);
			vectorfldnum++;
			break;
			default:
			break;
			}
	}

	// create an int array to store fld number which contains Vector100Dtype
	vectorfld = new int[vectorfldnum];
	for (int i = 0, j = 0; i < num_fld; i++)
	{
		if (fld_array[i] == 4)
		{
			vectorfld[j] = i;
			j++;
		}
	}

	// Create tuple for store data

	RID rid = new RID();
	try
	{
		t.setHdr((short) num_fld, attrType, null);
	} catch (InvalidTypeException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (InvalidTupleSizeException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	int size = t.size();
	t = new Tuple(size);
	try
	{
		t.setHdr((short) num_fld, attrType, null);
	} catch (InvalidTypeException | InvalidTupleSizeException | IOException e2)
	{
		// TODO Auto-generated catch block
		e2.printStackTrace();
	}
	// System.out.println("in phase2test tuple size ="+t.size());

	// Declare array, which contains va indexfile
	Vector100Key key = null;
	VAFile[] vafile = new VAFile[vectorfldnum];
	for (int i = 0; i < vectorfldnum; i++)
	{
		try
		{
			vafile[i] = new VAFile(DBNAME + "_" + Integer.toString(i), k);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	Vector100Dtype vector = new Vector100Dtype((short) 0);

	while (line != null)
	{
		cnt++;
		for (int i = 0; i < num_fld; i++)
		{
			try
			{
				data_str[i] = br.readLine();
				if (data_str[i] == null)
				{
					break;
				}
				switch (fld_array[i])
					{
					case 1:
					try
					{
						t.setIntFld(i + 1, Integer.parseInt(data_str[i].trim()));
					} catch (NumberFormatException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (FieldNumberOutOfBoundException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;

					case 2:
					try
					{
						t.setFloFld(i + 1, Float.parseFloat(data_str[i].trim()));
					} catch (NumberFormatException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (FieldNumberOutOfBoundException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
					case 3:
					// Temporary not implement
					break;
					case 4:
					String vectorStr[] = data_str[i].trim().split(" ");
					short vectordata[] = new short[100];
					if (vectorStr.length != 100)
					{
						System.out.print("not 100");
						break;
					}
					for (int i1 = 0; i1 < 100; i1++)
					{
						vectordata[i1] = Short.parseShort(vectorStr[i1].trim());
					}
					vector.setVectorValue(vectordata);

					try
					{
						t.set100DVectFld(i + 1, vector);
					} catch (FieldNumberOutOfBoundException e)
					{
						e.printStackTrace();
					}
					break;
					default:
					break;

					}

			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try
		{
			rid = f.insertRecord(t.getTupleByteArray());
		} catch (InvalidSlotNumberException | InvalidTupleSizeException
				| SpaceNotAvailableException | HFException | HFBufMgrException
				| HFDiskMgrException | IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (data_str[0] == null)
		{
			break;
		}
	}
	Scan scan = null;
	System.out.print("write page(data insertion) is " + PCounterPinPage.counter
			+ "\n");
	try
	{
		scan = new Scan(f);
	} catch (InvalidTupleSizeException | IOException e1)
	{
		e1.printStackTrace();
	}
	RID rid1 = new RID();
	Vector100Dtype[] vectorforIndex = new Vector100Dtype[vectorfldnum];
	try
	{
		Tuple tmp = scan.getNext(rid1);
		while (tmp != null)
		{
			t.tupleCopy(tmp);
			for (int i = 0; i < vectorfldnum; i++)
			{
				vectorforIndex[i] = t.get100DVectFld(vectorfld[i] + 1);
				try
				{
					key = new Vector100Key(vectorforIndex[i], k);
				} catch (VAException e1)
				{
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				try
				{
					vafile[i].insertKey(key, rid1);
				} catch (InvalidSlotNumberException
						| SpaceNotAvailableException | HFException
						| HFBufMgrException | HFDiskMgrException e)
				{
					e.printStackTrace();
				}
			}
			tmp = scan.getNext(rid1);
			if (tmp == null)
			{
				System.out.print("inser all");
			}
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
		System.out.print("The number of write page (key insertion) is "
				+ PCounterPinPage.counter + "\n");
		System.out.print("The number of Read page is " + PCounter.counter
				+ "\n");
		System.out.print("The number of write page in DB is "
				+ PCounterw.counter + "\n");
		scan.closescan();
		br.close();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	System.out.println("----------------- end test1-------------------------");
	return true;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test2()
	{
	System.out
			.println("----------------- begin test2-------------------------");
	// Open Query Specification file
	try
	{
		SystemDefs.JavabaseBM.flushAllPages();
	} catch (HashOperationException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (PageUnpinnedException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (PagePinnedException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (PageNotFoundException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (BufMgrException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	PCounter.setZero();
	PCounterw.setZero();
	PCounterPinPage.setZero();
	BufferedReader Querybr = null;
	try
	{
		Querybr = new BufferedReader(new FileReader(QSNAME));
	} catch (FileNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	String queryCommand = null;
	try
	{
		queryCommand = Querybr.readLine().trim();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	while (queryCommand != null)
	{
		// System.out.println(queryCommand);
		if (queryCommand.contains("NLJ"))
		{
			String ParamforRange;
			String ParamforNN;
			int D;
			try
			{
				ParamforRange = Querybr.readLine().trim();
				ParamforRange = ParamforRange.substring(0,
						ParamforRange.length() - 1);
				ParamforNN = Querybr.readLine().trim();
				ParamforNN = ParamforNN.substring(0, ParamforNN.length() - 1);
				D = Integer.parseInt(Querybr.readLine().trim());

				int beginindex = ParamforRange.indexOf("(");
				int arrayindexbegin = ParamforRange.indexOf("[");
				int arrayindexend = ParamforRange.indexOf("]");
				String ArgumentList[] = ParamforRange.substring(beginindex + 1,
						arrayindexbegin).split(",");
				String Outputfldsra[] = ParamforRange.substring(
						arrayindexbegin + 1, arrayindexend).split(",");
				RSIndexScan rscan = null;
				if (IndexOption == "Y")
				{
					rscan = rangescan(ArgumentList, Outputfldsra);
				}

				beginindex = ParamforNN.indexOf("(");
				arrayindexbegin = ParamforNN.indexOf("[");
				arrayindexend = ParamforNN.indexOf("]");
				ArgumentList = ParamforNN.substring(beginindex + 1,
						arrayindexbegin).split(",");
				String Outputfldsnn[] = ParamforNN.substring(
						arrayindexbegin + 1, arrayindexend).split(",");
				NNIndexScan nnscan = null;
				if (IndexOption == "Y")
				{
					nnscan = nnInexScan(ArgumentList, Outputfldsnn);
				}

				Tuple tmp = null;
				try
				{
					tmp = nnscan.get_next();
				} catch (IndexException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				AttrType outputflds[] = new AttrType[Outputfldsnn.length];
				for (int i = 0; i < outputflds.length; i++)
				{
					outputflds[i] = attrType[Integer.parseInt(Outputfldsnn[i]) - 1];
				}

				Tuple tt = new Tuple(tmp.size());
				try
				{
					tt.setHdr((short) outputflds.length, outputflds, null);
				} catch (InvalidTypeException | InvalidTupleSizeException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				tt.tupleCopy(tmp);
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
				System.out.print("}");
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		else if (queryCommand.contains("Range"))
		{
			System.out.println("in phase2test topk=" + topk + " bits=" + this.k
					+ " num of tuple=" + this.cnt + " NUMBUF=" + this.NUMBUF);
			int beginindex = queryCommand.indexOf("(");
			int arrayindexbegin = queryCommand.indexOf("[");
			int arrayindexend = queryCommand.indexOf("]");
			String ArgumentList[] = queryCommand.substring(beginindex + 1,
					arrayindexbegin).split(",");
			String Outputflds[] = queryCommand.substring(arrayindexbegin + 1,
					arrayindexend).split(",");
			int range = Integer.parseInt(ArgumentList[2]);
			if (IndexOption == "N")
			{
				Sort sort = sortIndex(ArgumentList, Outputflds, true);
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
						currentdistance = Vector100Dtype.distance(
								tmp.get100DVectFld(Q), TargetforSort);
						if (currentdistance >= range)
						{
							break;
						}
						System.out.print("\n\n\nResult Tuple " + resultnum
								+ ":\n{\n");
						AttrType outputflds[] = new AttrType[Outputflds.length];
						for(int i=0;i<outputflds.length;i++)
						{
							outputflds[i] = attrType[Integer.parseInt(Outputflds[i])-1];
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
								System.out.print(tt.getIntFld(Integer
										.parseInt(Outputflds[i + 1])));
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

				} catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else if (IndexOption == "Y")
			{
				RSIndexScan rscan = rangescan(ArgumentList, Outputflds);
				Tuple tmp = null;
				try
				{
					tmp = rscan.get_next();
					int resultnum = 0;
					while (tmp != null)
					{

						resultnum++;
						System.out.print("\n\n\nResult Tuple " + resultnum
								+ ":\n{\n");
						
						int size = tmp.size();
						AttrType outputflds[] = new AttrType[Outputflds.length];
						for (int i = 0; i < outputflds.length; i++)
						{
							outputflds[i] = attrType[Integer
									.parseInt(Outputflds[i]) - 1];
						}

						Tuple tt = new Tuple(size);
						try
						{
							tt.setHdr((short) outputflds.length, outputflds,
									null);
						} catch (InvalidTypeException
								| InvalidTupleSizeException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						tt.tupleCopy(tmp);
						for (int i = 0; i < outputflds.length; i++)
						{
							switch (outputflds[i].attrType)
								{
								case 1:
								System.out.print(tt.getIntFld(Integer
										.parseInt(Outputflds[i + 1])));
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
						System.out.print("}");
						tmp = rscan.get_next();
					}

				} catch (IndexException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FieldNumberOutOfBoundException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else if (queryCommand.contains("NN"))
		{
			int beginindex = queryCommand.indexOf("(");
			int arrayindexbegin = queryCommand.indexOf("[");
			int arrayindexend = queryCommand.indexOf("]");
			String ArgumentList[] = queryCommand.substring(beginindex + 1,
					arrayindexbegin).split(",");
			String Outputflds[] = queryCommand.substring(arrayindexbegin + 1,
					arrayindexend).split(",");

			if (IndexOption == "N")
			{
				AttrType outputflds[] = new AttrType[Outputflds.length];
				for (int i = 0; i < Outputflds.length; i++)
				{
					outputflds[i] = attrType[Integer.parseInt(Outputflds[i]) - 1];
				}
				Sort sort = sortIndex(ArgumentList, Outputflds, false);
				Tuple tmp = null;
				try
				{
					tmp = sort.get_next();
				} catch (SortException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknowAttrType e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (LowMemException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JoinsException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				Tuple JTuple = new Tuple();
				try
				{
					try
					{
						JTuple.setHdr((short) outputflds.length,
								outputflds, null);
					} catch (IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
				try
				{
					Projection.Project(tmp, attrType, JTuple, projlist,
							outputflds.length);
				} catch (UnknowAttrType | WrongPermat
						| FieldNumberOutOfBoundException | IOException e1)
				{
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				int curtupnum = 0;
				while (tmp != null && topk > 0)
				{
					topk--;
					curtupnum++;
					System.out.print("\n\n\n\nTuple" + curtupnum + ":\n{");
					try
					{
						Projection.Project(tmp, attrType, JTuple, projlist,
								outputflds.length);
					} catch (UnknowAttrType | WrongPermat
							| FieldNumberOutOfBoundException | IOException e1)
					{
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					for (int i = 0; i < Outputflds.length; i++)
					{
						switch (outputflds[i].attrType)
							{
							case 1:
							try
							{
								System.out.print(JTuple.getIntFld(i + 1));
								System.out.print(",\n");
							} catch (NumberFormatException
									| FieldNumberOutOfBoundException
									| IOException e)
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;
							case 2:
							try
							{
								System.out.print(JTuple.getFloFld(i + 1));
								System.out.print(",\n");
							} catch (NumberFormatException
									| FieldNumberOutOfBoundException
									| IOException e)
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							break;
							case 5:
							System.out.print("[");
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
							System.out.print("],\n");
							break;
							}
					}
					System.out.print("}\n");
					try
					{
						tmp = sort.get_next();
					} catch (SortException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnknowAttrType e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (LowMemException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JoinsException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e)
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
			// Here we use NNIndexScan
			else
			{
				NNIndexScan nnscan = nnInexScan(ArgumentList, Outputflds);
				AttrType tmpattr[] = new AttrType[Outputflds.length];
				for (int i = 0; i < Outputflds.length; i++)
				{
					tmpattr[i] = attrType[Integer.parseInt(Outputflds[i]) - 1];
				}
				try
				{
					Tuple tmp = nnscan.get_next();
					if (tmp != null)
					{
						Tuple t = new Tuple(tmp.size());
						try
						{
							t.setHdr((short) Outputflds.length, tmpattr, null);
						} catch (InvalidTypeException
								| InvalidTupleSizeException | IOException e)
						{
							e.printStackTrace();
						}
						while (tmp != null)
						{
							t.tupleCopy(tmp);
							System.out.print("\n\n\n{\n");
							for (int i = 0; i < tmpattr.length; i++)
							{
								switch (tmpattr[i].attrType)
									{
									case 1:
									try
									{
										System.out.print(t.getIntFld(i + 1));
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
										System.out.print(t.getFloFld(i + 1));
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
									System.out.print("[");
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
									System.out.print("],\n");
									break;
									}
							}
							tmp = nnscan.get_next();
						}
						System.out.print("}");

					}
				} catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		try
		{
			queryCommand = Querybr.readLine();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	try
	{
		Querybr.close();
	} catch (IOException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	System.out.println("Read page in test2=" + PCounter.counter);
	System.out.println("Write page in test2=" + PCounterw.counter);
	System.out.print("The number of pin page in BM is"
			+ PCounterPinPage.counter + "\n");

	System.out.println("----------------- end test2-------------------------");
	return true;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test3()
	{
	return true;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test4()
	{
	return true;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test5()
	{
	return true;
	}

	/**
	 * @return whether the test has completely successfully
	 */
	protected boolean test6()
	{
	return true;
	}

}

public class phase2test
{
	public static void main(String argv[])
	{
	boolean phase2status;
	Phase2Driver phase2 = new Phase2Driver();

	phase2status = phase2.runTests();

	if (phase2status != true)
	{
		System.out.println("Error ocurred during index tests");
	}
	else
	{
		System.out.println("phase2 tests completed successfully");
	}
	}
}
