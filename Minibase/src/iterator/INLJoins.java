package iterator;

import global.AttrType;
import global.IndexType;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

public class INLJoins extends Iterator
{
	
	private AttrType      _in1[],  _in2[];
	  private   int        in1_len, in2_len;
	  private   Iterator  outer;
	  private   short t2_str_sizescopy[];
	  private   CondExpr OutputFilter[];
	  private   CondExpr RightFilter[];
	  private   int        n_buf_pgs;        // # of buffer pages available.
	  private   boolean        done,         // Is the join complete
	  get_from_outer;                 // if TRUE, a tuple is got from outer
	  private   Tuple     outer_tuple, inner_tuple;
	  private   Tuple     Jtuple;           // Joined tuple
	  private   FldSpec   perm_mat[];
	  private   int        nOutFlds;
	  private   Heapfile  hf;
	  private   Scan      inner;
	  //Add indextype
	  private IndexType indextype;
	  private java.lang.String _indexname;
	/**constructor
	   *Initialize the two relations which are joined, including relation type,
	   *@param in1  Array containing field types of R.
	   *@param len_in1  # of columns in R.
	   *@param t1_str_sizes shows the length of the string fields.
	   *@param in2  Array containing field types of S
	   *@param len_in2  # of columns in S
	   *@param  t2_str_sizes shows the length of the string fields.
	   *@param amt_of_mem  IN PAGES
	   *@param am1  access method for left i/p to join
	   *@param relationName  access hfapfile for right i/p to join
	   *@param outFilter   select expressions
	   *@param rightFilter reference to filter applied on right i/p
	   *@param proj_list shows what input fields go where in the output tuple
	   *@param n_out_flds number of outer relation fileds
	   *@exception IOException some I/O fault
	   *@exception NestedLoopException exception from this class
	   */
	public INLJoins( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,
			   IndexType index,
			   java.lang.String indexName,
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   )
	{
		
	}
	
	
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception
	{
	// TODO Auto-generated method stub
	return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException
	{
	// TODO Auto-generated method stub
	
	}

}
