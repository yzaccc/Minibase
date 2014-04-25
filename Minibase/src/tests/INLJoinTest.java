package tests;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.INLJoins;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

class INLJoinTestDriver extends TestDriver
{
	public INLJoinTestDriver(){
		super("");
	}
	public void runTest() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		SystemDefs sysdef = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
		AttrType [] in1 = new AttrType[4];
		in1[0] = new AttrType(2);
		in1[1] = new AttrType(5);
		in1[2] = new AttrType(2);
		in1[3] = new AttrType(5);
		int len_in1 = 4;
		AttrType [] in2 = new AttrType[1];
		in2[0] = new AttrType(5);
		int len_in2 = 1;
		int amt_of_mem = 40;
		short t1_str_sizes[]=null;
		short t2_str_sizes[]=null;
		Heapfile hf = new Heapfile("rel1");
		FileScan fscan = null;
		FldSpec[] proj_list = new FldSpec[4];
		for (int i = 0; i < 4; i++)
		{
			proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		fscan = new FileScan("rel1", in1, null, (short) 4,
				4, proj_list, null);
//		Tuple t = fscan.get_next();
//		t.setHdr((short)4, in1, null);
//		t.get100DVectFld(2).printVector();
		String relationName = "rel2";
		IndexType indextype =new IndexType(1);
		String indexName = "VABTreeIndex_1_B_16";
		CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopLE);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);
		expr[0].distance = 0;
		expr[0].next = null;
		expr[1] = null;
		CondExpr[] expr1 = null;
		FldSpec[] projlist = new FldSpec[5];
		for (int i = 0; i < 4; i++)
		{
			projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		projlist[4] = new FldSpec(new RelSpec(RelSpec.innerRel),1);
		INLJoins inlj = new INLJoins(in1, len_in1, t1_str_sizes,
				in2, len_in2, t2_str_sizes, amt_of_mem,
				fscan, relationName, indextype,
				indexName, expr1,
				expr, projlist, 5);
		Tuple t1 = inlj.get_next();
		AttrType [] in11 = new AttrType[5];
		in11[0] = new AttrType(2);
		in11[1] = new AttrType(5);
		in11[2] = new AttrType(2);
		in11[3] = new AttrType(5);
		in11[4] = new AttrType(5);
		t1.setHdr((short) 5, in11, null);
		t1.get100DVectFld(2).printVector();
	}
}
public class INLJoinTest{
	public static void main(String argv[]) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
	{
		INLJoinTestDriver inlj = new INLJoinTestDriver();
		inlj.runTest();
	}
	
}