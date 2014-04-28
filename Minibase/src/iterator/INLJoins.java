package iterator;

import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;

import VAIndex.VAFile;
import VAIndex.Vector100Key;
import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.KeyClass;
import btree.KeyDataEntry;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;

public class INLJoins extends Iterator
{

	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator outer;
	private short t2_str_sizescopy[];
	private CondExpr OutputFilter[];
	private CondExpr RightFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private boolean done, // Is the join complete
			get_from_outer; // if TRUE, a tuple is got from outer
	private Tuple outer_tuple, inner_tuple;
	private Tuple Jtuple; // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;
	private Heapfile hf;
	private BTreeFile btf = null;
	private VAFile vaf = null;
	private Scan inner;
	private IndexScan iscan = null;
	// Add indextype
	private KeyDataEntry innerEntry;
	private String _relname;
	private IndexType _indextype;
	private java.lang.String _indexname;
	private int bitnum;
	private Tuple tmpInnerTuple = null;
	private Vector100Dtype outerVector, innerVector;
	private int _b;

	/**
	 * constructor Initialize the two relations which are joined, including
	 * relation type,
	 * 
	 * @param in1
	 *            Array containing field types of R.
	 * @param len_in1
	 *            # of columns in R.
	 * @param t1_str_sizes
	 *            shows the length of the string fields.
	 * @param in2
	 *            Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param t2_str_sizes
	 *            shows the length of the string fields.
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left i/p to join
	 * @param relationName
	 *            access hfapfile for right i/p to join
	 * @param outFilter
	 *            select expressions
	 * @param rightFilter
	 *            reference to filter applied on right i/p
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @exception IOException
	 *                some I/O fault
	 * @exception NestedLoopException
	 *                exception from this class
	 * @throws AddFileEntryException
	 * @throws ConstructPageException
	 * @throws GetFileEntryException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws HFException
	 */
	public INLJoins(AttrType in1[], int len_in1, short t1_str_sizes[],
			AttrType in2[], int len_in2, short t2_str_sizes[], int amt_of_mem,
			Iterator am1, String relationName, IndexType index,
			java.lang.String indexName, CondExpr outFilter[],
			CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds)
			throws IOException, NestedLoopException, GetFileEntryException,
			ConstructPageException, AddFileEntryException, HFException,
			HFBufMgrException, HFDiskMgrException
		{
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2;
		_indextype = index;
		_indexname = indexName;
		outer = am1;
		t2_str_sizescopy = t2_str_sizes;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter = rightFilter;

		n_buf_pgs = amt_of_mem;
		inner = null;
		done = false;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		_relname = relationName;
		try
		{
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1,
					in2, len_in2, t1_str_sizes, t2_str_sizes, proj_list,
					nOutFlds);
		} catch (TupleUtilsException e)
		{
			throw new NestedLoopException(e,
					"TupleUtilsException is caught by NestedLoopsJoins.java");
		}

		try
		{
			hf = new Heapfile(relationName);
		} catch (Exception e)
		{
			throw new NestedLoopException(e, "Create new heapfile failed.");
		}
		// Check whether we can use index file for the inner loop
		if (index == null && indexName == null)
		{
			System.out
					.println("NO index file available, use heapfile scan for inner join");
		}
		else
		{
			// Create indexfile for innerloop
			bitnum = Integer.parseInt(Character.toString(_indexname
					.charAt(_indexname.length() - 1)));
			if (IndexType.B_Index == index.indexType)
			{
				_indexname = indexName;
			}
			else if (IndexType.VAIndex == index.indexType)
			{
				vaf = new VAFile(_indexname, bitnum);
				_b = Integer.parseInt(_indexname.split("_")[3]);
			}
			else
			{
				System.out.println("IndexType not supported");
			}
		}

		}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception
	{
	// // This is a DUMBEST form of a join, not making use of any key
	// information...
	if (_indextype == null && _indexname == null)
	{
		if (done)
			return null;

		do
		{
			// If get_from_outer is true, Get a tuple from the outer, delete
			// an existing scan on the file, and reopen a new scan on the file.
			// If a get_next on the outer returns DONE?, then the nested loops
			// join is done too.

			if (get_from_outer == true)
			{
				get_from_outer = false;
				if (inner != null) // If this not the first time,
				{
					// close scan
					inner = null;
				}

				try
				{
					inner = hf.openScan();
				} catch (Exception e)
				{
					throw new NestedLoopException(e, "openScan failed");
				}

				if ((outer_tuple = outer.get_next()) == null)
				{
					done = true;
					if (inner != null)
					{
						outer.close();
						inner.closescan();
						inner = null;
					}

					return null;
				}
			} // ENDS: if (get_from_outer == TRUE)

			// The next step is to get a tuple from the inner,
			// while the inner is not completely scanned && there
			// is no match (with pred),get a tuple from the inner.

			RID rid = new RID();
			while ((inner_tuple = inner.getNext(rid)) != null)
			{
				inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
				if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
				{
					if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple,
							_in1, _in2) == true)
					{
						// Apply a projection on the outer and inner tuples.
						Projection.Join(outer_tuple, _in1, inner_tuple, _in2,
								Jtuple, perm_mat, nOutFlds);
						return Jtuple;
					}
				}
			}

			// There has been no match. (otherwise, we would have
			// returned from t//he while loop. Hence, inner is
			// exhausted, => set get_from_outer = TRUE, go to top of loop

			get_from_outer = true; // Loop back to top and get next outer tuple.
		} while (true);
	}
	// Using BTree Index;
	else if (IndexType.B_Index == _indextype.indexType && _indexname.contains("VABTreeIndex"))
	{
		if (done)
			return null;
		AttrType[] attrType = new AttrType[1];
		attrType[0] = new AttrType(AttrType.attrVector100D);
		short[] attrSize = new short[1];
		attrSize[0] = 200;

		do
		{
			if (get_from_outer == true)
			{
				get_from_outer = false;
				if (iscan != null) // If this not the first time,
				{
					iscan.close();
					iscan = null;
				}
				FldSpec[] projlist = new FldSpec[1];
				projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
				iscan = new IndexScan(new IndexType(IndexType.B_Index),
						_relname, _indexname, attrType, attrSize, 1, 1, projlist,
						null, 1, true);

				if ((outer_tuple = outer.get_next()) == null)
				{
					done = true;
					outer.close();
					iscan.close();
					iscan = null;
					return null;
				}
			}
			RID rid = new RID();
			KeyClass key1 = null;
			Vector100Key vkey2 = null;
			int indexApproximateDistance = 0;
			int realDistance = 0;
			outer_tuple.setHdr((short) _in1.length, _in1, null);
			while ((key1 = iscan.get_nextKey(rid)) != null)
			{
				//outerVector = outer_tuple
				//		.get100DVectFld(RightFilter[0].operand1.symbol.offset);
				outerVector = outer_tuple.get100DVectFld(2);
				if (key1 instanceof Vector100Key)
				{
					vkey2 = (Vector100Key) key1;
				}
				indexApproximateDistance = vkey2
						.getLowerBoundDistance(outerVector);
				if (indexApproximateDistance <= RightFilter[0].distance)
				{
					tmpInnerTuple = hf.getRecord(rid);
					tmpInnerTuple.setHdr((short) _in2.length, _in2, null);
					innerVector = tmpInnerTuple
							.get100DVectFld(RightFilter[0].operand2.symbol.offset);
					realDistance = Vector100Dtype.distance(outerVector,
							innerVector);
					if (realDistance <= RightFilter[0].distance)
					{
//						System.out.println("Real distance is "+realDistance);
//						System.out.print("innerVector is");
//						innerVector.printVector();
//						System.out.print("\n");
//						System.out.print("outerVector is");
//						outerVector.printVector();
//						System.out.print("\n");
						if (PredEval.Eval(OutputFilter, outer_tuple,
								inner_tuple, _in1, _in2) == true)
						{
							// Apply a projection on the outer and inner tuples.
							Projection.Join(outer_tuple, _in1, tmpInnerTuple,
									_in2, Jtuple, perm_mat, nOutFlds);
							return Jtuple;
						}
					}
				}
			}
			get_from_outer = true; // Loop back to top and get next outer tuple.
		} while (true);
	}
	else if (IndexType.VAIndex == _indextype.indexType)
	{
		if (done)
			return null;

		do
		{
			// If get_from_outer is true, Get a tuple from the outer, delete
			// an existing scan on the file, and reopen a new scan on the file.
			// If a get_next on the outer returns DONE?, then the nested loops
			// join is done too.

			if (get_from_outer == true)
			{
				get_from_outer = false;
				if (inner != null) // If this not the first time,
				{
					inner = null;
				}

				try
				{
					inner = vaf.openScan();
				} catch (Exception e)
				{
					throw new NestedLoopException(e, "openScan failed");
				}

				if ((outer_tuple = outer.get_next()) == null)
				{
					done = true;
					if (inner != null)
					{
						inner.closescan();
						outer.close();
						inner = null;
					}

					return null;
				}
			} // ENDS: if (get_from_outer == TRUE)

			// The next step is to get a tuple from the inner,
			// while the inner is not completely scanned && there
			// is no match (with pred),get a tuple from the inner.

			RID rid = new RID();
			AttrType[] vaindex = new AttrType[1];
			vaindex[0] = new AttrType(6); // 6 represent vaindex
			int realDistance = 0;
			while ((inner_tuple = inner.getNext(rid)) != null)
			{
				short[] attrSize = new short[1];
				attrSize[0] = (short)(Vector100Key.getVAKeyLength(this._b)+8);
				inner_tuple.setHdr((short) 1, vaindex, attrSize);
				outerVector = outer_tuple
						.get100DVectFld(RightFilter[0].operand1.symbol.offset);
				Vector100Key key = inner_tuple.get100DVectKeyFld(1).key;
				key.setAllRegionNumber();
				int indexApproximateDistance = key.getLowerBoundDistance(outerVector);
				if (indexApproximateDistance <= RightFilter[0].distance)
				{
					tmpInnerTuple = hf.getRecord(inner_tuple.get100DVectKeyFld(1).rid);
					tmpInnerTuple.setHdr((short) _in2.length, _in2, null);
					innerVector = tmpInnerTuple
							.get100DVectFld(RightFilter[0].operand2.symbol.offset);
//					System.out.print("innerVector is");
//					innerVector.printVector();
					realDistance = Vector100Dtype.distance(outerVector,
							innerVector);
					if (realDistance <= RightFilter[0].distance)
					{
						if (PredEval.Eval(OutputFilter, outer_tuple,
								inner_tuple, _in1, _in2) == true)
						{
							// Apply a projection on the outer and inner tuples.
//							System.out.println("Real distance is "+realDistance);
//							System.out.print("innerVector is");
//							innerVector.printVector();
//							System.out.print("\n");
//							System.out.print("outerVector is");
//							outerVector.printVector();
//							System.out.print("\n");
							AttrType [] JTupleAttr = new AttrType[nOutFlds];
							for(int i=0;i<_in1.length;i++)
							{
								JTupleAttr[i] = new AttrType(_in1[i].attrType);
							}
							for(int i=_in1.length;i<nOutFlds;i++)
							{
								JTupleAttr[i] = new AttrType(_in2[i-_in1.length].attrType);
							}
							Jtuple.setHdr((short)nOutFlds, JTupleAttr, null);
							Projection.Join(outer_tuple, _in1, tmpInnerTuple,
									_in2, Jtuple, perm_mat, nOutFlds);
							System.out.println("JTuple?");
							Jtuple.get100DVectFld(5).printVector();
							return Jtuple;
						}
					}
				}
			}

			// There has been no match. (otherwise, we would have
			// returned from t//he while loop. Hence, inner is
			// exhausted, => set get_from_outer = TRUE, go to top of loop

			get_from_outer = true; // Loop back to top and get next outer tuple.
		} while (true);
	}
	else
	{
		System.out.print("Input error, Join error");
		return null;
	}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException
	{

	}
}
