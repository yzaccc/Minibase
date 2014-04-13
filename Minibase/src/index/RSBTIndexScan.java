package index;

import VAIndex.Vector100Key;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.PageId;
import global.RID;
import global.Vector100Dtype;

public class RSBTIndexScan {

	IndexScan iscan = null;
	private Tuple Jtuple;// result tuple
	Vector100Dtype _query;
	int _distance;
	int profld;// field number after projection

	/**
	 * 
	 * @param index
	 * @param relName
	 * @param indName
	 * @param types
	 * @param str_sizes
	 * @param noInFlds
	 * @param noOutFlds
	 * @param outFlds
	 * @param selects
	 *            should be empty
	 * @param fldNum
	 * @param query
	 * @param distance
	 */
	public RSBTIndexScan(IndexType index, String relName, String indName,
			AttrType[] types, short[] str_sizes, int noInFlds, int noOutFlds,
			FldSpec[] outFlds, CondExpr[] selects, int fldNum,
			Vector100Dtype query, int distance, int b) {
		
		for (int i=0;i<noOutFlds;i++)
		{
			if (outFlds[i].offset == fldNum)
			{
				profld = i+1;
//				System.out.println("in range scan btree profld ="+profld);
			}
		}
		this._distance = distance;
		this._query = new Vector100Dtype(query.getVectorValue());

		short[] ts_sizes;// no use
		Jtuple = new Tuple();
		AttrType[] Jtypes = new AttrType[noOutFlds];

		try {
			ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types,
					noInFlds, str_sizes, outFlds, noOutFlds);
		} catch (Exception e) {
			e.printStackTrace();
		}

		Vector100Dtype low_vec = Vector100Dtype.getLowerBoundVector(query,
				distance,b);
		Vector100Dtype high_vec = Vector100Dtype.getUpperBoundVector(query,
				distance,b);
		Vector100Key lowkey = null;
		Vector100Key highkey = null;
		try {
			lowkey = new Vector100Key(low_vec, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			highkey = new Vector100Key(high_vec, b);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// low_vec.printVector();

		// high_vec.printVector();
		System.out.println("lowkey in rs");
		lowkey.printAllRegionNumber();
		System.out.println("highkey in rs");
		highkey.printAllRegionNumber();

		CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopGE);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrVector100Dkey);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				fldNum);
		expr[0].operand2.vectorkey = lowkey;// lower bound
		expr[0].next = null;
		// expr[1] = new CondExpr();
		// expr[1].op = new AttrOperator(AttrOperator.aopLE);
		// expr[1].type1 = new AttrType(AttrType.attrSymbol);
		// expr[1].type2 = new AttrType(AttrType.attrVector100Dkey);
		// expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
		// fldNum);
		// expr[1].operand2.vectorkey=highkey;// upper bound
		// expr[1].next = null;
		// expr[2] = null;
		expr[1] = null;

		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), relName,
					indName, types, str_sizes, noInFlds, noOutFlds, outFlds,
					expr, fldNum, false);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Tuple get_next(RID rid) {
		Tuple tmp = null;
		try {
			tmp = iscan.get_next(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (tmp != null) {
			try {
				Jtuple.tupleCopy(tmp);
			} catch (Exception e) {
				e.printStackTrace();
			}
			Vector100Dtype tmpVec = null;
			try {
				tmpVec = Jtuple.get100DVectFld(profld);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (Vector100Dtype.distance(tmpVec, this._query) < this._distance)
				return tmp;
			// current one is out of range, get next tuple
			try {
				tmp = iscan.get_next(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return null;

	}

}
