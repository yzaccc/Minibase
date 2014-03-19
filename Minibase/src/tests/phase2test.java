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
import java.io.PrintWriter;
import java.util.ArrayList;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import diskmgr.PCounterPinPage;
import diskmgr.PCounterw;
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

	public String fld_str[];
	public static String DATAFILENAME;// =
									  // "/home/jinxuanw/demo_data/test_data.txt";
	public static String DBNAME;// = "test1.in";
	public static String QSNAME;// = "/home/jinxuanw/demo_data/nljquery12.txt";
	public static String IndexOption;// = "Y";
	public static AttrType[] attrType;
	public static int k = 10;// number of bits input
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
	public boolean IsNN = false;
	public int num_fld;

	public Sort sortIndex(String[] ArgumentList, String[] Outputflds,
			boolean IsRange)
	{
	System.out.print("In Sort\n");
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
		if (IsRange == true)
			sort = new Sort(attrType, (short) attrType.length, null, fscan, Q,
					order[0], Vector100Dtype.Max * 2, phase2test.numbuf,
					phase2test.TargetforSort, 0);
		else
			sort = new Sort(attrType, (short) attrType.length, null, fscan, QA,
					order[0], Vector100Dtype.Max * 2, phase2test.numbuf,
					phase2test.TargetforSort, topk);
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
		if (phase2test.query == true)
			;
		else
		{
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}

	} catch (IOException e)
	{
		System.err.println("" + e);
	}

	boolean _pass = runAllTests();

	try
	{
		if (phase2test.query == true)
			;
		else
		{
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
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
	if (phase2test.batchinsert == false)
		return true;

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
	String DBFileSpec = dbpath + DBNAME + "_spec";
	PrintWriter specfile = null;
	try
	{
		specfile = new PrintWriter(DBFileSpec);
	} catch (Exception e)
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
	specfile.println(line.trim());
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
	specfile.println(line.trim());
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
		try
		{
			SystemDefs.JavabaseBM.flushAllPages();
		} catch (HashOperationException | PageUnpinnedException
				| PagePinnedException | PageNotFoundException | BufMgrException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		scan.closescan();
		br.close();
		specfile.close();
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
	// Check if it is query, if not doesn't execute this function
	if (phase2test.query == false)
		return true;
	System.out
			.println("----------------- begin test2-------------------------");

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

	// Specbr is to read a spec file in /tmp, which contains number of fields
	// and type.
	// Querbr is to read QSPEC
	BufferedReader Querybr = null;
	BufferedReader Specbr = null;
	try
	{
		Querybr = new BufferedReader(new FileReader(QSNAME));
		Specbr = new BufferedReader(new FileReader(dbpath + DBNAME + "_spec"));
	} catch (FileNotFoundException e)
	{
		e.printStackTrace();
	}
	String queryCommand = null;
	try
	{
		num_fld = Integer.parseInt(Specbr.readLine().trim());
	} catch (NumberFormatException | IOException e2)
	{
		// TODO Auto-generated catch block
		e2.printStackTrace();
	}
	try
	{
		fld_str = Specbr.readLine().trim().split(" ");
	} catch (IOException e2)
	{
		// TODO Auto-generated catch block
		e2.printStackTrace();
	}
	short[] fld_array = new short[fld_str.length];
	for (int i = 0; i < fld_str.length; i++)
	{
		fld_array[i] = Short.parseShort(fld_str[i]);
	}
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
	vectorfld = new int[vectorfldnum];
	for (int i = 0, j = 0; i < num_fld; i++)
	{
		if (fld_array[i] == 4)
		{
			vectorfld[j] = i;
			j++;
		}
	}
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
	// Begin to loop over the QSpec file, check if it is NlJ, NN or Range

	while (queryCommand != null)
	{
		if (queryCommand.contains("NLJ"))
		{
			String ParamforRange;
			String ParamforNN;
			try
			{
				// Parse Input
				ParamforRange = Querybr.readLine().trim();
				ParamforRange = ParamforRange.substring(0,
						ParamforRange.length() - 1);
				ParamforNN = Querybr.readLine().trim();
				ParamforNN = ParamforNN.substring(0, ParamforNN.length() - 1);
				phase2test.D = Integer.parseInt(Querybr.readLine().trim());

				int beginindex = ParamforRange.indexOf("(");
				int arrayindexbegin = ParamforRange.indexOf("[");
				int arrayindexend = ParamforRange.indexOf("]");
				String ArgumentListra[] = ParamforRange.substring(
						beginindex + 1, arrayindexbegin).split(",");
				String Outputfldsrastr = ParamforRange.substring(
						arrayindexbegin + 1, arrayindexend);
				String Outputfldsra[] = Outputfldsrastr.split(",");
				int range = Integer.parseInt(ArgumentListra[2]);
				RSIndexScan rscan = null;
				Q = Integer.parseInt(ArgumentListra[0]);

				// Using scan
				if (IndexOption.equals("Y"))
				{
					rscan = rangescan(ArgumentListra, Outputfldsra);
				}
				int raVindex = 0;
				// A array contains output data type for range search
				AttrType outputfldTypera[] = new AttrType[Outputfldsra.length];
				for (int i = 0; i < Outputfldsra.length; i++)
				{
					if (Integer.parseInt(Outputfldsra[i]) == Q)
						raVindex = i + 1;
					outputfldTypera[i] = attrType[Integer
							.parseInt(Outputfldsra[i]) - 1];
				}
				// Parase input for NN
				beginindex = ParamforNN.indexOf("(");
				arrayindexbegin = ParamforNN.indexOf("[");
				arrayindexend = ParamforNN.indexOf("]");
				String ArgumentListnn[] = ParamforNN.substring(beginindex + 1,
						arrayindexbegin).split(",");
				String Outputfldsnn[] = ParamforNN.substring(
						arrayindexbegin + 1, arrayindexend).split(",");
				NNIndexScan nnscan = null;
				QA = Integer.parseInt(ArgumentListnn[0]);
				phase2test.topk = Integer.parseInt(ArgumentListnn[2]);
				AttrType outputfldTypenn[] = new AttrType[Outputfldsnn.length];

				// Using nnscan
				if (IndexOption.equals("Y"))
				{
					nnscan = nnInexScan(ArgumentListnn, Outputfldsnn);
				}
				int nnVindex = 0;
				for (int i = 0; i < Outputfldsnn.length; i++)
				{
					if (Integer.parseInt(Outputfldsnn[i]) == QA)
						nnVindex = i + 1;
					outputfldTypenn[i] = attrType[Integer
							.parseInt(Outputfldsnn[i]) - 1];
				}

				// Create Outputflds array for final output
				int totalnumOutputfld = Outputfldsnn.length
						+ Outputfldsra.length;
				AttrType NLJOutputflds[] = new AttrType[totalnumOutputfld];
				FldSpec[] proj_list = new FldSpec[totalnumOutputfld];
				int fldindex = 0;
				for (fldindex = 0; fldindex < Outputfldsra.length; fldindex++)
				{
					proj_list[fldindex] = new FldSpec(
							new RelSpec(RelSpec.outer), fldindex + 1);
					NLJOutputflds[fldindex] = attrType[Integer
							.parseInt(Outputfldsra[fldindex]) - 1];
				}
				for (; fldindex < totalnumOutputfld; fldindex++)
				{
					proj_list[fldindex] = new FldSpec(new RelSpec(
							RelSpec.innerRel), fldindex - Outputfldsra.length
							+ 1);
					NLJOutputflds[fldindex] = attrType[Integer
							.parseInt(Outputfldsnn[fldindex
									- Outputfldsra.length]) - 1];
				}

				TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

				// Create Outputfilter
				CondExpr[] outFilter = new CondExpr[2];
				outFilter[0] = new CondExpr();
				outFilter[1] = new CondExpr();
				outFilter[0].next = null;
				outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
				outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
				outFilter[0].operand1.symbol = new FldSpec(new RelSpec(
						RelSpec.outer), raVindex);
				outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				outFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), nnVindex);
				outFilter[1] = null;
				// SortMerge sm = null;
				// try
				// {
				// sm = new SortMerge(outputfldTypera, outputfldTypera.length,
				// null, outputfldTypenn, outputfldTypenn.length,
				// null, raVindex, 200, nnVindex, 200, NUMBUF, rscan,
				// nnscan, true, false, ascending, outFilter,
				// proj_list, proj_list.length);
				// } catch (Exception e)
				// {
				// System.err.println("" + e);
				// }

				ArrayList<Tuple> tuplennlist = new ArrayList<Tuple>();
				ArrayList<Tuple> tupleralist = new ArrayList<Tuple>();
				if (IndexOption.equals("Y"))
				{

					Tuple tmpnn = nnscan.get_next();
					while (tmpnn != null)
					{
						tuplennlist.add(new Tuple());
						tuplennlist.get(tuplennlist.size() - 1).setHdr(
								(short) outputfldTypenn.length,
								outputfldTypenn, null);
						tuplennlist.get(tuplennlist.size() - 1)
								.tupleCopy(tmpnn);
						tmpnn = nnscan.get_next();
					}
					Tuple tmpra = rscan.get_next();

					while (tmpra != null)
					{
						tupleralist.add(new Tuple());
						tupleralist.get(tupleralist.size() - 1).setHdr(
								(short) outputfldTypera.length,
								outputfldTypera, null);
						tupleralist.get(tupleralist.size() - 1)
								.tupleCopy(tmpra);
						tmpra = rscan.get_next();
					}
				}
				else
				{
					Sort sortra = sortIndex(ArgumentListra, Outputfldsra, true);
					Tuple tmpra = new Tuple();
					int currentdistance = 0;
					try
					{
						tmpra = sortra.get_next();
						int resultnum = 0;
						while (currentdistance >= 0 && tmpra != null)
						{

							resultnum++;
							tmpra.setHdr((short) attrType.length, attrType,
									null);
							currentdistance = Vector100Dtype.distance(
									tmpra.get100DVectFld(Q),
									phase2test.TargetforSort);
							if (currentdistance >= range)
							{
								while (tmpra != null)
									tmpra = sortra.get_next();
								break;
							}
							AttrType outputflds[] = new AttrType[outputfldTypera.length];
							for (int i = 0; i < outputflds.length; i++)
							{
								outputflds[i] = outputfldTypera[i];
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
							FldSpec[] projlist = new FldSpec[outputfldTypera.length];
							RelSpec rel = new RelSpec(RelSpec.outer);
							for (int i = 0; i < outputflds.length; i++)
							{
								projlist[i] = new FldSpec(rel,
										Integer.parseInt(Outputfldsra[i]));
							}
							Projection.Project(tmpra, attrType, JTuple,
									projlist, outputflds.length);
							tupleralist.add(new Tuple());
							tupleralist.get(tupleralist.size() - 1).setHdr(
									(short) outputfldTypera.length,
									outputfldTypera, null);
							tupleralist.get(tupleralist.size() - 1).tupleCopy(
									JTuple);
							tmpra = sortra.get_next();
						}

					} catch (Exception e)
					{
						e.printStackTrace();
					}
					while (tmpra != null)
						sortra.get_next();
					sortra.close();
					SystemDefs.JavabaseBM.flushAllPages();
					// Begin Sort NN, and add it to NNList
					AttrType outputflds[] = new AttrType[Outputfldsnn.length];
					IsNN = true;
					for (int i = 0; i < Outputfldsnn.length; i++)
					{
						outputflds[i] = attrType[Integer
								.parseInt(Outputfldsnn[i]) - 1];
					}
					Sort sortnn = sortIndex(ArgumentListnn, Outputfldsnn, false);
					Tuple tmpnn = new Tuple();
					for (int i = 0; i < phase2test.topk; i++)
					{
						tmpnn = sortnn.get_next();
						Tuple JTuple = new Tuple();
						JTuple.setHdr((short) outputflds.length, outputflds,
								null);
						FldSpec[] projlist = new FldSpec[Outputfldsnn.length];
						RelSpec rel = new RelSpec(RelSpec.outer);
						for (int i1 = 0; i1 < outputflds.length; i1++)
						{
							projlist[i1] = new FldSpec(rel,
									Integer.parseInt(Outputfldsnn[i1]));
						}
						try
						{
							Projection.Project(tmpnn, attrType, JTuple,
									projlist, outputflds.length);
						} catch (UnknowAttrType | WrongPermat
								| FieldNumberOutOfBoundException | IOException e1)
						{
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						tuplennlist.add(new Tuple());
						tuplennlist.get(tuplennlist.size() - 1).setHdr(
								(short) outputfldTypenn.length,
								outputfldTypenn, null);
						tuplennlist.get(tuplennlist.size() - 1).tupleCopy(
								JTuple);
					}
					while (tmpnn != null)
						tmpnn = sortnn.get_next();
					sortnn.close();
					SystemDefs.JavabaseBM.flushAllPages();
				}

				ArrayList<Tuple> resultTupleList = new ArrayList<Tuple>();
				AttrType vectortype = new AttrType(AttrType.attrVector100D);
				Tuple tmpresult = new Tuple();
				tmpresult.setHdr((short) NLJOutputflds.length, NLJOutputflds,
						null);
				phase2test.NLJSortNNFlag = true;
				for (int i = 0; i < tupleralist.size(); i++)
				{
					for (int j = 0; j < tuplennlist.size(); j++)
					{
						phase2test.NLJSortNNFlag = true;
						int distance = TupleUtils.CompareTupleWithTuple(
								vectortype, tupleralist.get(i), raVindex,
								tuplennlist.get(j), nnVindex)
								- phase2test.D;
						if (distance <= 0)
						{
							int curposition = 1;
							for (int k = 0; k < tupleralist.get(i).fldCnt; k++)
							{
								curposition++;
								switch (outputfldTypera[k].attrType)
									{
									case 1:
									tmpresult
											.setIntFld(k + 1, tupleralist
													.get(i).getIntFld(k + 1));
									break;
									case 2:
									tmpresult
											.setFloFld(k + 1, tupleralist
													.get(i).getFloFld(k + 1));
									break;
									case 5:
									tmpresult.set100DVectFld(k + 1, tupleralist
											.get(i).get100DVectFld(k + 1));
									break;
									}
							}

							for (int k = 0; k < tuplennlist.get(j).fldCnt; k++, curposition++)
							{
								switch (outputfldTypera[k].attrType)
									{
									case 1:
									tmpresult
											.setIntFld(curposition, tuplennlist
													.get(j).getIntFld(k + 1));
									break;
									case 2:
									tmpresult
											.setFloFld(curposition, tuplennlist
													.get(j).getFloFld(k + 1));
									break;
									case 5:
									tmpresult.set100DVectFld(
											curposition,
											tuplennlist.get(j).get100DVectFld(
													k + 1));
									break;
									}
							}
							resultTupleList.add(new Tuple());
							resultTupleList.get(resultTupleList.size() - 1)
									.setHdr((short) NLJOutputflds.length,
											NLJOutputflds, null);
							resultTupleList.get(resultTupleList.size() - 1)
									.tupleCopy(tmpresult);
						}

					}
				}

				for (int j = 0; j < resultTupleList.size(); j++)
				{
					int cur = j + 1;
					System.out.print("\n\n\nTuple " + cur + "{\n");
					for (int i = 0; i < NLJOutputflds.length; i++)
					{
						switch (NLJOutputflds[i].attrType)
							{
							case 1:
							System.out.print(resultTupleList.get(j).getIntFld(
									i + 1));
							System.out.print(",\n");
							break;
							case 2:
							System.out.print(resultTupleList.get(j).getFloFld(
									i + 1));
							System.out.print(",\n");
							break;
							case 5:
							System.out.print("[");
							resultTupleList.get(j).get100DVectFld(i + 1)
									.printVector();
							System.out.print("],\n");
							break;
							}
					}
					System.out.print("}\n");

				}
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
			Q = Integer.parseInt(ArgumentList[0]);
			int range = Integer.parseInt(ArgumentList[2]);
			if (IndexOption.equals("N"))
			{
				System.out.print("Using Sort\n");
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
						currentdistance = Vector100Dtype
								.distance(tmp.get100DVectFld(Q),
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
			else if (IndexOption.equals("Y"))
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
			QA = Integer.parseInt(ArgumentList[0]);
			phase2test.topk = Integer.parseInt(ArgumentList[2]);
			topk = Integer.parseInt(ArgumentList[2]);
			if (IndexOption.equals("N"))
			{
				System.out.print("Using Sort\n");
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
						JTuple.setHdr((short) outputflds.length, outputflds,
								null);
					} catch (IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (InvalidTypeException | InvalidTupleSizeException e)
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
				while (tmp != null)
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
				try
				{
					sort.close();
					try
					{
						SystemDefs.JavabaseBM.flushAllPages();
					} catch (HashOperationException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (PageUnpinnedException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (PagePinnedException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (PageNotFoundException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (BufMgrException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (SortException | IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
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
						int currentnum = 1;
						while (tmp != null && topk >= 0)
						{
							topk--;
							t.tupleCopy(tmp);
							System.out.print("\n\n\nTuple " + currentnum
									+ ":\n{\n");
							currentnum++;
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
							System.out.print("}");
							tmp = nnscan.get_next();
						}

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
	public static int D = -1;
	public static int topk;
	public static Vector100Dtype TargetforSort;
	public static Vector100Dtype TargetforNN;
	public static Vector100Dtype TargetforRange;
	public static boolean NLJSortNNFlag = false;
	public static int numbuf = 80;
	public static boolean batchinsert = false;
	public static boolean query = false;

	public static void main(String argv[])
	{
	boolean phase2status;
	System.out.print(argv.length + "\n");
	Phase2Driver phase2 = new Phase2Driver();
	if (argv.length == 4)
	{
		query = true;
		Phase2Driver.DBNAME = argv[0];
		Phase2Driver.QSNAME = argv[1];
		Phase2Driver.IndexOption = argv[2];
		numbuf = Integer.parseInt(argv[3]);
	}
	else if (argv.length == 2)
	{
		batchinsert = true;
		Phase2Driver.DATAFILENAME = argv[0];
		Phase2Driver.DBNAME = argv[1];
	}
	else
	{
		System.out.print("Input error");
	}
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
