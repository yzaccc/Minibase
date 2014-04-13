package tests;

import java.io.*;

import global.*;
import VAIndex.NNIndexScan;
import VAIndex.RSIndexScan;
import VAIndex.VAException;
import VAIndex.VAFile;
import VAIndex.VAFileScan;
import VAIndex.Vector100Key;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import btree.*;

import java.util.Arrays;
import java.util.Random;

class Index2Driver extends TestDriver implements GlobalConst {

	private static String data1[] = { "raghu", "xbao", "cychan", "leela",
			"ketola", "soma", "ulloa", "dhanoa", "dsilva", "kurniawa",
			"dissoswa", "waic", "susanc", "kinc", "marc", "scottc", "yuc",
			"ireland", "rathgebe", "joyce", "daode", "yuvadee", "he",
			"huxtable", "muerle", "flechtne", "thiodore", "jhowe", "frankief",
			"yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch", "binh",
			"dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
			"chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
			"newell", "schiesl", "neuman", "heitzman", "wan", "gunawan",
			"djensen", "juei-wen", "josephin", "harimin", "xin", "zmudzin",
			"feldmann", "joon", "wawrzon", "yi-chun", "wenchao", "seo",
			"karsono", "dwiyono", "ginther", "keeler", "peter", "lukas",
			"edwards", "mirwais", "schleis", "haris", "meyers", "azat",
			"shun-kit", "robert", "markert", "wlau", "honghu", "guangshu",
			"chingju", "bradw", "andyw", "gray", "vharvey", "awny", "savoy",
			"meltz" };

	private static String data2[] = { "andyw", "awny", "azat", "barthel",
			"binh", "bloch", "bradw", "chingju", "chui", "chung-pi", "cychan",
			"dai", "daode", "dhanoa", "dissoswa", "djensen", "dsilva",
			"dwiyono", "edwards", "evgueni", "feldmann", "flechtne",
			"frankief", "ginther", "gray", "guangshu", "gunawan", "hai",
			"handi", "harimin", "haris", "he", "heitzman", "honghu",
			"huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
			"jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc",
			"kurniawa", "leela", "lukas", "mak", "marc", "markert", "meltz",
			"meyers", "mirwais", "muerle", "muthiah", "neuman", "newell",
			"peter", "raghu", "randal", "rathgebe", "robert", "savoy",
			"schiesl", "schleis", "scottc", "seo", "shi", "shun-kit",
			"siddiqui", "soma", "sonthi", "sungk", "susanc", "tak", "thiodore",
			"ulloa", "vharvey", "waic", "wan", "wawrzon", "wenchao", "wlau",
			"xbao", "xiaoming", "xin", "yi-chun", "yiching", "yuc", "yung",
			"yuvadee", "zmudzin" };

	private static int NUM_RECORDS = data2.length;
	private static int LARGE = 1000;
	private static short REC_LEN1 = 32;
	private static short REC_LEN2 = 160;

	public Index2Driver() {
		super("indextest");
	}

	public boolean runTests() {

		System.out
				.println("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath, 300, NUMBUF, "Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		// This step seems redundant for me. But it's in the original
		// C++ code. So I am keeping it as of now, just in case I
		// I missed something
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		// Run the tests. Return type different from C++
		boolean _pass = runAllTests();

		// Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		System.out.println("\n" + "..." + testName() + " tests ");
		System.out.println(_pass == OK ? "completely successfully" : "failed");
		System.out.println(".\n\n");

		return _pass;
	}

	// Insert record to a new file
	protected boolean test1() {
		System.out
				.println("------------------------ TEST 1 --------------------------");
		boolean status = OK;
		
		
		int bitnum = 4;

		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100D);
		short[] attrSize = new short[1];
		attrSize[0] = 200;


		FldSpec[] projlist = new FldSpec[1];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);

		Vector100Dtype vector1 = new Vector100Dtype((short) 1);// data
		Vector100Dtype vector2 = new Vector100Dtype((short) 9999);// data
		Vector100Dtype vector3 = new Vector100Dtype((short) 6666);// data
		Vector100Dtype vector4 = new Vector100Dtype((short) -1);// data
		Vector100Dtype target = new Vector100Dtype((short) 5);// target
		Vector100Dtype[] vectorObject = new Vector100Dtype[4];
		vectorObject[0] = vector1;
		vectorObject[1] = vector2;
		vectorObject[2] = vector3;
		vectorObject[3] = vector4;
		
		Vector100Key vkey = null;
		
		try {
			vkey = new Vector100Key(vectorObject[0], bitnum);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}		
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrVector100Dkey);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.vectorkey=vkey;
		expr[0].next = null;
		expr[1] = null;

		// set tuple header for vector 100
		Tuple t1 = new Tuple();
		try {
			t1.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		int size = t1.size();

		// Create heapfile
		RID rid = null;
		Heapfile hf = null;
		try {
			hf = new Heapfile("test1.in");
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		// set header again??
		t1 = new Tuple(size);
		try {
			t1.setHdr((short) 1, attrType, attrSize);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		

		BTreeFile btf = null;
		try {
			btf = new BTreeFile("VA_BTreeIndex", AttrType.attrVector100Dkey,
					Vector100Key.getVAKeyLength(bitnum), 1/* delete */);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		System.out.println("BTreeIndex created successfully.\n");

		for (int i = 0; i < 4; i++) {
			try {
				t1.set100DVectFld(1, vectorObject[i%4]);
				// System.out.println("offset "+t.getOffset());
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				// System.out.println("fldCnt in test5 "+t.getLength());
				// System.out.println("before ");
				// System.out.println("before "+
				// Arrays.toString(t.returnTupleByteArray()));
				rid = hf.insertRecord(t1.returnTupleByteArray());
				System.out.println("in IndexTest rid " + rid.slotNo + " "
						+ rid.pageNo.pid);// debug
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			try {
				vkey = new Vector100Key(vectorObject[i%4], bitnum);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			try {
				btf.insert(vkey, rid);

			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			// try delete
			if (i==3 && false)
			{
				try {
					btf.Delete(vkey, rid);

				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				
			}
		}
		RSBTIndexScan rsscan = new RSBTIndexScan(new IndexType(IndexType.B_Index), "test1.in",
					"VA_BTreeIndex", attrType, attrSize, 1, 1, projlist, expr, 1,
					target,200,bitnum);
		
		
		Tuple tmptuple = null;
		RID rid1  = new RID(new PageId(-1),-1);
		try {
			tmptuple = rsscan.get_next(rid1);
			
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		
		
		int cnt=0;
		Vector100Dtype tmpVec = null;
		while (tmptuple != null) {
			cnt++;
			try {
				t1.tupleCopy(tmptuple);

				tmpVec = t1.get100DVectFld(1);
//				System.out.println("in index test 4 ");// debug
				System.out.println(" index2 test1 range rid "+rid1.pageNo+" "+rid1.slotNo);
				tmpVec.printVector();// debug
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				tmptuple = rsscan.get_next(rid1);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}

		System.err
				.println("------------------- TEST 1 completed ---------------------\n");

		return status;
	}

	protected boolean test2() {
		System.out
				.println("------------------------ TEST 2 --------------------------");

		boolean status = OK;

		

		System.err
				.println("------------------- TEST 2 completed ---------------------\n");

		return status;
	}

	protected boolean test3() {
		System.out
				.println("------------------------ TEST 3 --------------------------");

		boolean status = OK;

		

		System.err
				.println("------------------- TEST 3 completed ---------------------\n");

		return status;
	}

	protected boolean test4() {

		System.out
				.println("------------------------ TEST 4 test--------------------------");
		System.out
				.println("------------------------ NN Scan test--------------------------");

		boolean status = OK;

		

		return true;
	}

	protected boolean test5() {
		System.out
				.println("------------------------ TEST 5 test--------------------------");
		System.out
				.println("------------------------ range Scan test--------------------------");


		return true;
	}

	protected boolean test6() {
		System.out
				.println("------------------------ TEST 6 test--------------------------");
		System.out
				.println("------------------------ BTree range Scan test--------------------------");

		boolean status = OK;
		
		
		
		return true;
	}

	protected String testName() {
		return "Index";
	}
}

public class Index2Test {
	public static void main(String argv[]) {
		boolean indexstatus;

		Index2Driver indext = new Index2Driver();

		indexstatus = indext.runTests();
		if (indexstatus != true) {
			System.out.println("Error ocurred during index tests");
		} else {
			System.out.println("Index tests completed successfully");
		}
	}
}