package tests;

import index.IndexException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import diskmgr.PCounter;
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

class Phase2Driver extends TestDriver implements GlobalConst{
	public Phase2Driver(){
		super("phase2test");
	}
	public static String DATAFILENAME = "/Users/akun1012/Desktop/jinxuanw/data.txt";
	public static String DBNAME = "test1.in";
	public static String QSNAME ="/Users/akun1012/Desktop/jinxuanw/qspec.txt";
	public static String IndexOption = "Y";
	public static AttrType[] attrType;
	public static int k = 4;// number of bits   input
	public static int numtuple = 400;// input
	public Tuple t = new Tuple();
	public int vectorfld [];
	public int topk;
	public int cnt = 0;// number of lines of input
	Heapfile f = null;
	public boolean runTests(){
		System.out.println("\n" + "Running " + testName() + " tests...." +"\n");
		SystemDefs sysdef = new SystemDefs(dbpath,300,NUMBUF,"Clock");
		
		//Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf";
		
		newdbpath = dbpath;
		newlogpath = logpath;
		
		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;
		
		try{
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}catch( IOException e)
		{
			System.err.println("" + e);
		}
		
		boolean _pass = runAllTests();
		
		try{
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}catch(IOException e){
			System.err.println("" + e);
		}
		
		System.out.println("\n" + "..." + testName()+" tests ");
		System.out.println(_pass == OK ? "completely successfully" : "failed");
		System.out.println(".\n\n");

		return _pass;
	}
	
	 /** 
	   * @return whether the test has completely successfully 
	   */
	protected boolean test1()
	{
		System.out.println("----------------- begin test1-------------------------");
		//PCounter.initialize();
		PCounter.setZero();
		PCounterw.setZero();
	int num_fld;
	BufferedReader br = null;
	try
	{
		br = new BufferedReader(new FileReader(DATAFILENAME)); //Create buffer to read DATAFILENAME
	} catch (FileNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	// read num of fields
	String line = null;
	try
	{
		line = br.readLine(); //Read first line
	} catch (IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	num_fld = Integer.parseInt(line.trim()); // Set number of fields

	
	// read array type of fields
	try
	{
		line = br.readLine(); //Read Second Line, which contains numbers specify fields
	} catch (IOException e1)
	{
		e1.printStackTrace();
	}
	
	//Assign the second line of DATAFILE into fld_array
	String fld_str[] = line.split(" ");
	short[] fld_array = new short[fld_str.length];
	for (int i = 0; i < fld_str.length; i++)
	{
		fld_array[i] = Short.parseShort(fld_str[i]);
	}
	
	
	
	try
	{
		 f = new Heapfile(DBNAME); //This should be the DBNAME
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	
	// Read and stored
	String data_str[] = new String[num_fld];
	
	attrType= new AttrType[num_fld];
	int vectorfldnum = 0;
	for(int i=0;i<num_fld;i++)
	{
		switch (fld_array[i]){
		case 1:
			attrType[i] = new AttrType(AttrType.attrInteger);
			break;
		case 2:
			attrType[i] = new AttrType(AttrType.attrReal);
			break;
		case 3:
			//currently Sample data don't have string, so ignore it.
			break;
		case 4:
			attrType[i] = new AttrType(AttrType.attrVector100D);
			vectorfldnum++;
			break;
		default:
			break;
		}
	}
	
	//create an int array to store fld number which contains Vector100Dtype
	vectorfld =new int[vectorfldnum]; 
	for(int i=0,j=0;i<num_fld;i++)
	{
		if(fld_array[i] == 4)
		{
			vectorfld[j] = i;
			j++;
		}
	}
	
	//Create tuple for store data
	
	RID rid = new RID();
	try
	{
		t.setHdr((short)num_fld, attrType, null);
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
		t.setHdr((short)num_fld, attrType, null);
	} catch (InvalidTypeException | InvalidTupleSizeException | IOException e2)
	{
		// TODO Auto-generated catch block
		e2.printStackTrace();
	}
	
	//Declare array, which contains va indexfile
	Vector100Key key =  null;
	VAFile [] vafile = new VAFile[vectorfldnum];
	for(int i=0;i<vectorfldnum;i++)
	{
		try
		{
			vafile[i] = new VAFile(DBNAME+"_" + Integer.toString(i), k);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//	VAFile vafile [] = new VAFile[num_fld];
//	for(int i=0;i<num_fld;i++)
//	{
//		if(fld_array[i] == 4)
//		{
//			vafile[i] = new VAFile(DBNAME+"_" + Integer.toString(i), k);
//		}
//		else
//			vafile[i]
//	}
	Vector100Dtype vector = new Vector100Dtype((short)0);
	//PCounter.counter = 0;
	//Read Data one by one
	int kk=numtuple*4;//limit read line

	while ((kk)!=0 && (line != null))
	{
		kk--;
		cnt++;
		for (int i = 0; i < num_fld; i++)
		{
			try
			{
				data_str[i] = br.readLine();
				if(data_str[i] == null)
				{
					break;
				}
				switch (fld_array[i]){
				case 1:
    				try
    				{
    					t.setIntFld(i+1, Integer.parseInt(data_str[i].trim()));
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
    					t.setFloFld(i+1, Float.parseFloat(data_str[i].trim()));
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
				//Temporary not implement
				break;
				case 4:
    				String vectorStr[] = data_str[i].trim().split(" ");
    				short vectordata[] = new short[100];
    				if(vectorStr.length!= 100)
    				{
    					System.out.print("not 100");
    					break;
    				}
    				for(int i1=0;i1<100;i1++){
    					vectordata[i1] = Short.parseShort(vectorStr[i1].trim());
    				}
    				 vector.setVectorValue(vectordata);
    				
    				try
    				{
    					t.set100DVectFld(i+1, vector);
    					//System.out.print(vectordata[99]);
    				} catch (FieldNumberOutOfBoundException e)
    				{
    					// TODO Auto-generated catch block
    					//System.out.print(i+"\n");
    					//System.out.print(data_str[i]);
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
				| SpaceNotAvailableException | HFException
				| HFBufMgrException | HFDiskMgrException | IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(data_str[0] == null)
		{
			break;
		}
	}
	Scan scan = null;
	System.out.print("write page1 is " + PCounterw.counter +"\n");
//	PCounter.counter = 0;
	try
	{
		 scan = new Scan(f);
	} catch (InvalidTupleSizeException | IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	RID rid1 = new RID();
	Vector100Dtype [] vectorforIndex = new Vector100Dtype[vectorfldnum];
	try
	{
		Tuple tmp = scan.getNext(rid1);
		while(tmp != null)
		{
			t.tupleCopy(tmp);
			for(int i=0;i<vectorfldnum;i++)
			{
				vectorforIndex[i] = t.get100DVectFld(vectorfld[i]);
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			tmp = scan.getNext(rid1);
		}
	} catch (InvalidTupleSizeException | IOException e1)
	{
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (FieldNumberOutOfBoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try
	{
		//System.out.print("Page used is" +PCounter.counter);
		System.out.print("write page2 is " + PCounterw.counter +"\n");
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
	  protected boolean test2 () { 
		  System.out.println("----------------- begin test2-------------------------");
	  	//Open Query Specification file
		  PCounter.setZero();
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
	  	while(queryCommand != null)
	  	{
//	  		System.out.println(queryCommand);
	  		if(queryCommand.contains("Range"))
	  		{
	  			int beginindex = queryCommand.indexOf("(");
	  			int arrayindexbegin = queryCommand.indexOf("[");
	  			int arrayindexend = queryCommand.indexOf("]");	  			
	  			String ArgumentList[] = queryCommand.substring(beginindex+1, arrayindexbegin).split(",");
                String Outputflds [] = queryCommand.substring(arrayindexbegin+1, arrayindexend).split(",");
                int Q=Integer.parseInt(ArgumentList[0]);
	  			String T = ArgumentList[1];
	  			int range = Integer.parseInt(ArgumentList[2]);
	  			
	  			//Create an Vector100Dtype to store the target.
	  			BufferedReader Targetbr =null;
	  			try
				{
					Targetbr = new BufferedReader(new FileReader(T));
				} catch (FileNotFoundException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			String[] TargetStr = new String [100];
				try
				{
					TargetStr = Targetbr.readLine().trim().split(" ");
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			short[] Targetarray = new short[100];
	  			for(int i=0;i<100;i++)
	  			{
	  				Targetarray[i] = Short.parseShort(TargetStr[i]);
	  			}
	  			TupleUtils.target = new Vector100Dtype(Targetarray);//Target Vector done
	  			try
				{
					Targetbr.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			
	  			//Create ProjectList, Or output flds
	  			FldSpec[] projlist = new FldSpec[Outputflds.length];
	  			RelSpec rel = new RelSpec(RelSpec.outer);
	  			for(int i=0;i<Outputflds.length;i++)
	  			{
	  				projlist[i] = new FldSpec(rel, Integer.parseInt(Outputflds[i]));
	  			}
	  			
	  			//Create RangeIndex Scan
	  			RSIndexScan rscan = null;
	  			short [] strsize =null;
	  			CondExpr[] selects =null;
	  			int index = 0;
	  			for(int i=0;i<vectorfld.length;i++)
	  			{
	  				if(vectorfld[i] == Q-1)
	  				{
	  					index = i;
	  					break;
	  				}
	  					
	  			}
	  			try
				{
					rscan =  new RSIndexScan(new IndexType(IndexType.VAIndex),
							DBNAME, 
							DBNAME+"_"+index, 
							attrType,
							strsize,
							attrType.length, 
							Outputflds.length, 
							projlist,
							selects,
							Q,
							TupleUtils.target, 
							range, k);
				} catch (IndexException | VAException
						| FieldNumberOutOfBoundException | IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			Tuple tmp = null;
	  			try
				{
					tmp = rscan.get_next();
//					int size = tmp.size();
//					Tuple tt = new Tuple(size);
//					tt.setHdr(numFlds, types, strSizes);
					t.get100DVectFld(2).printVector();;
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
	  		else if(queryCommand.contains("NN"))
	  		{
	  			int beginindex = queryCommand.indexOf("(");
	  			int arrayindexbegin = queryCommand.indexOf("[");
	  			int arrayindexend = queryCommand.indexOf("]");	  			
	  			String ArgumentList[] = queryCommand.substring(beginindex+1, arrayindexbegin).split(",");
                String Outputflds [] = queryCommand.substring(arrayindexbegin+1, arrayindexend).split(",");
                int QA=Integer.parseInt(ArgumentList[0]);
	  			String T = ArgumentList[1];
	  			int topk = Integer.parseInt(ArgumentList[2]);
	  			
	  			System.out.println("in phase2test topk="+topk+" bits="+this.k+
	  					" num of tuple="+this.cnt+" NUMBUF="+this.NUMBUF);
	  			
	  			BufferedReader Targetbr =null;
	  			try
				{
					Targetbr = new BufferedReader(new FileReader(T));
				} catch (FileNotFoundException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			String[] TargetStr = new String [100];
				try
				{
					TargetStr = Targetbr.readLine().trim().split(" ");
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			short[] Targetarray = new short[100];
	  			for(int i=0;i<100;i++)
	  			{
	  				Targetarray[i] = Short.parseShort(TargetStr[i]);
	  			}
	  			TupleUtils.target = new Vector100Dtype(Targetarray);//Target Vector done
	  			try
				{
					Targetbr.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  			
	  			//Create ProjectList, Or output flds
	  			FldSpec[] projlist = new FldSpec[Outputflds.length];
	  			RelSpec rel = new RelSpec(RelSpec.outer);
	  			for(int i=0;i<Outputflds.length;i++)
	  			{
	  				projlist[i] = new FldSpec(rel, Integer.parseInt(Outputflds[i]));
	  			}
	  			
	  			short [] strsize =null;
	  			CondExpr[] selects =null;
	  			
	  			if(IndexOption == "N")
	  			{
	  				FileScan fscan =null;
	  				try
					{
						fscan=  new FileScan(DBNAME, attrType, strsize, (short)attrType.length,Outputflds.length ,
								projlist, null);
					} catch (FileScanException | TupleUtilsException
							| InvalidRelation | IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	  				TupleOrder[] order = new TupleOrder[2];
	  				order[0] = new TupleOrder(TupleOrder.Ascending);
	  				order[1] = new TupleOrder(TupleOrder.Descending);
	  				try
					{
						Sort sort = new Sort(attrType, (short) attrType.length, strsize, fscan, 2, order[0],
								Vector100Dtype.Max*2, 300, TupleUtils.target, topk );
						try
						{
							sort.get_next().get100DVectFld(4).printVector();
						} catch (Exception e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						};
					} catch (SortException | IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	  			}
	  			
	  			
	  			
	  			
	  			
	  			else
	  			{
    	  			NNIndexScan nnscan  = null;
    	  			
    	  			RSIndexScan rscan = null;
    	  			int index = 0;
    	  			for(int i=0;i<vectorfld.length;i++)
    	  			{
    	  				if(vectorfld[i] == QA-1)
    	  				{
    	  					index = i;
    	  					break;
    	  				}
    	  					
    	  			}
    	  			
    	  			try{
    	  				nnscan = new NNIndexScan(new IndexType(IndexType.VAIndex),
    	  						DBNAME,
    	  						DBNAME+"_"+index,
    	  						attrType,
    	  						strsize,
    	  						4,
    	  						Outputflds.length,
    	  						projlist,
    	  						selects,
    	  						QA,
    	  						TupleUtils.target,topk,k);
    	  				
    	  			}catch (Exception e) {
    	  				e.printStackTrace();
    	  			}
    	  			AttrType tmpattr [] = new AttrType [Outputflds.length];
    	  			for(int i=0;i<Outputflds.length;i++)
    	  			{
    	  				tmpattr[i]= attrType[Integer.parseInt(Outputflds[i])-1];
    	  			}
    	  			try
    				{
    					Tuple tmp = nnscan.get_next();
    					Tuple t = new Tuple(tmp.size());
    					try
    					{
    						t.setHdr((short)Outputflds.length, tmpattr, null);
    					} catch (InvalidTypeException | InvalidTupleSizeException
    							| IOException e)
    					{
    						// TODO Auto-generated catch block
    						e.printStackTrace();
    					}
    					t.tupleCopy(tmp);
//    					try
//    					{
//    						t.get100DVectFld(2).printVector();
//    					} catch (FieldNumberOutOfBoundException | IOException e)
//    					{
//    						// TODO Auto-generated catch block
//    						e.printStackTrace();
//    					}
    					
    					for(int i=0;i<topk-1;i++)
    					{
    						tmp = nnscan.get_next();
    						t.tupleCopy(tmp);
//    						try
//    						{
//    							t.get100DVectFld(2).printVector();
//    						} catch (FieldNumberOutOfBoundException | IOException e)
//    						{
//    							// TODO Auto-generated catch block
//    							e.printStackTrace();
//    						}
    					}
    				} catch (IndexException e)
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
	  	System.out.println("PCounter in test2="+PCounter.counter);
	  	System.out.println("----------------- end test2-------------------------");
	  	return true;
	  }


	/** 
	   * @return whether the test has completely successfully 
	   */
	  protected boolean test3 () { return true; }

	  /** 
	   * @return whether the test has completely successfully 
	   */
	  protected boolean test4 () { return true; }

	  /** 
	   * @return whether the test has completely successfully 
	   */
	  protected boolean test5 () { return true; }

	  /** 
	   * @return whether the test has completely successfully 
	   */
	  protected boolean test6 () { return true; }
	  
}
public class phase2test
{
	public static void main(String argv[]) 
	{
	boolean phase2status;
	Phase2Driver phase2 = new Phase2Driver();
	
	phase2status = phase2.runTests();
	
	if (phase2status != true) {
		System.out.println("Error ocurred during index tests");
	} else {
		System.out.println("phase2 tests completed successfully");
	}
	}
}
