package VAIndex;

import java.io.IOException;

import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

public class VAFileScan extends Scan{
	
	public VAFileScan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}
	
	public Tuple getNextVA(RID rid) 
		    throws InvalidTupleSizeException,
			   IOException{
		
		Tuple tuple = getNext(rid);
		return tuple;
	}
	
	

}
