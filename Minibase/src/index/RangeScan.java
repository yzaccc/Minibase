package index;

import java.io.IOException;

import VAIndex.RSIndexScan;
import VAIndex.VAException;
import VAIndex.VAFile;
import VAIndex.Vector100Key;
import btree.KeyClass;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class RangeScan extends Iterator
{
	private String _indexname;
	private short _attrSize;
	private FldSpec[] projlist;
	private String _hfName;
	private Heapfile _hf;
	private IndexScan iscan;
	private int _distance;
	private Vector100Dtype _TargetVector;
	private Tuple t;
	private String _indexType;
	private int _fldnum;
	private VAFile vaf;
	private RSIndexScan rscan;
	private RSBTIndexScan rsbtscan;
	private int _bitnum;
	private Tuple _tmp = null;
	private AttrType [] _attr;
	public RangeScan(String indexFileName, int Fldnum, String indexType,
			String HeapfileName, AttrType[] attr, int distance,
			Vector100Dtype TargetVector, int bitnum)
		{
		// VABTreeIndexrel1_2_B_16
		_indexname = indexFileName;
		_indexType = indexType;
		_fldnum = Fldnum;
		_hfName = HeapfileName;
		int numFlds = attr.length;
		_attr = new AttrType[numFlds];
		t = new Tuple();
		for(int i=0;i<numFlds;i++)
		{
			_attr[i] = new AttrType(attr[i].attrType);
		}
		try
		{
			t.setHdr((short)numFlds, _attr, null);
		} catch (InvalidTypeException | InvalidTupleSizeException
				| IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (_indexType.equals("B"))
		{
			short[] attrSize = new short[1];
			attrSize[0] = 200;
			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			
			try
			{
				_hf = new Heapfile(HeapfileName);
			} catch (HFException | HFBufMgrException | HFDiskMgrException
					| IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			AttrType[] attrIndexType = new AttrType[1];
			attrIndexType[0] = new AttrType(AttrType.attrVector100D);
			_distance = distance;
			_TargetVector = TargetVector;

			try
			{
				iscan = new IndexScan(new IndexType(IndexType.B_Index),
						_hfName, _indexname, attrIndexType, attrSize, 1, 1,
						projlist, null, 1, true);
			} catch (IndexException | InvalidTypeException
					| InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (_indexType.equals("H"))
		{
			_bitnum = bitnum;
			try
			{
				vaf = new VAFile(_indexname, _bitnum);
			} catch (HFException | HFBufMgrException | HFDiskMgrException
					| IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			short[] strsize = null;
			CondExpr[] selects = null;
			FldSpec[] projlist = new FldSpec[attr.length];
			for (int i = 0; i < attr.length; i++)
			{
				projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			}

			try
			{
				rscan = new RSIndexScan(new IndexType(IndexType.VAIndex),
						_hfName, _indexname, attr, strsize, attr.length,
						attr.length, projlist, selects, _fldnum, TargetVector,
						_distance, _bitnum);
			} catch (IndexException | VAException
					| FieldNumberOutOfBoundException | IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	// TODO Auto-generated method stub
	if (_indexType.equals("B"))
	{
		RID rid = new RID();
		rsbtscan = new RSBTIndexScan(new IndexType(IndexType.B_Index),
				_hfName, _indexname, _attr, null, _attr.length, _attr.length,
				projlist, null, Integer.parseInt(_indexname.split("_")[3]), _TargetVector, _distance, _bitnum);
		_tmp = rsbtscan.get_next(rid);
		while (_tmp != null)
		{
			t.tupleCopy(_tmp);
			_tmp = rscan.get_next();
			return t;
		}
		return null;
//		KeyClass key1 = null;
//		Vector100Key vkey2 = null;
//		int indexApproximateDistance = 0;
//		int realDistance = 0;
//		//System.out.println("Range scan begin");
//		while ((key1 = iscan.get_nextKey(rid)) != null)
//		{
//			if (key1 instanceof Vector100Key)
//			{
//				vkey2 = (Vector100Key) key1;
//			}
//			indexApproximateDistance = vkey2
//					.getLowerBoundDistance(_TargetVector);
//			if (indexApproximateDistance <= _distance)
//			{
//				_tmp = _hf.getRecord(rid);
//				t.tupleCopy(_tmp);
//				realDistance = Vector100Dtype.distance(
//						t.get100DVectFld(_fldnum), _TargetVector);
//				if (realDistance <= _distance)
//				{
//					return t;
//				}
//			}
//		}
//		//System.out.println("Range scan finished");
//		iscan.close();
//		return null;
	}
	else if (_indexType.equals("H"))
	{
		_tmp = rscan.get_next();
		while (_tmp != null)
		{
			t.tupleCopy(_tmp);
			_tmp = rscan.get_next();
			return t;
		}
		return null;
	}
	else
	{
		System.out.println("IndexType not recognized");
		return _tmp;
	}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException
	{

	}

}
