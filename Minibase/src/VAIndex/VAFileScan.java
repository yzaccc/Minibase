package VAIndex;
 
import index.IndexException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.TupleUtilsException;
 
import java.io.IOException;
import java.util.ArrayList;
 
import btree.KeyClass;
import global.AttrType;
import global.PageId;
import global.RID;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
 
public class VAFileScan extends FileScan {
    private boolean isInit = false;
    private Tuple tuplein;// input tuple
    private Vector100Dtype target;
    private int _count;
    private int _count2;
    private Heapfile hf;// data file
//  private Scan hfscan;
    private VACandidate vac[] = null;
    private ArrayList<VACandidate> vac2;
    private int scanType = 0;// 1 for nn, 2 for range
    private int _fldNum ;
    private int nextidx = 0;
 
    public VAFileScan(String  file_name,
            AttrType in1[],                
            short s1_sizes[], 
            short     len_in1,              
            int n_out_flds,
            FldSpec[] proj_list,
            CondExpr[]  outFilter                   
            ) throws InvalidTupleSizeException,
            IOException, FileScanException, TupleUtilsException, InvalidRelation {
        super(file_name,in1,s1_sizes,len_in1,n_out_flds,proj_list,outFilter);
    }
 
    public Tuple getNextVA(RID rid) throws InvalidTupleSizeException,
            IOException {
 
        if (this.isInit == false){
            System.err.println("VAFileScan not inited");
            return null;
        }
        if (this.nextidx >= this._count2)
            return null;
        Tuple tuple = null;
        if (this.scanType == 1)
        {
            tuple = vac[nextidx].getTuple();
        }
        else if (this.scanType == 2)
        {
            tuple = vac2.get(nextidx).getTuple();
        }
        this.nextidx++;
        return tuple;
    }
 
    public void initVA(AttrType[] types,
            short[] str_sizes, int noInFlds, int fldNum) throws IndexException {
 
        this._fldNum = fldNum;
 
        tuplein = new Tuple();
        try {
            tuplein.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: setHdr error");
        }
        int t1_size = tuplein.size();// input tuple size
        // set again make sure it is correct
        tuplein = new Tuple(t1_size);
        try {
            tuplein.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
        this.isInit = true;
 
    }
 
    public void VAFileRangeScan(KeyClass key, int distance) {
        if (!(key instanceof Vector100DataKey)) {
            System.err.println("wrong key type in VAFileScan");
            return;
        }
        this.scanType = 2;
        this.target = ((Vector100DataKey) key).getVec();
        RID rid = new RID();
        Tuple temp = null;
        Vector100Dtype tmpVec = null;
        try {
            temp = this.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (temp != null) {
            tuplein.tupleCopy(temp);
            try{
                tmpVec = tuplein.get100DVectFld(_fldNum);// get indexed field
            }catch (Exception e) {
 
                e.printStackTrace();
            }
            int realdistance = Vector100Dtype.distance(this.target, tmpVec);
            if (realdistance < distance){
                VACandidate tmp = new VACandidate(realdistance, rid,tmpVec,tuplein);
                vac2.add(tmp);
                this._count2++;
            }
        }
    }
    public void VAFileNNScan(KeyClass key, int count) {
        if (!(key instanceof Vector100DataKey)) {
            System.err.println("wrong key type in VAFileScan");
            return;
        }
        this.scanType = 1;
        this.target = ((Vector100DataKey) key).getVec();
        this._count = count;
        int largestdistance = initCandidate();
        RID rid = new RID();
        Tuple temp = null;
        Vector100Dtype tmpVec = null;
        try {
            temp = this.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (temp != null) {
            tuplein.tupleCopy(temp);
            try{
                tmpVec = tuplein.get100DVectFld(_fldNum);// get indexed field
            }catch (Exception e) {
 
                e.printStackTrace();
            }
            int realdistance = Vector100Dtype.distance(this.target, tmpVec);
            if (realdistance < largestdistance){
                largestdistance = Candidate(realdistance,rid,tmpVec,tuplein);
                this._count2++;
            }
 
        }
 
    }
    private int initCandidate(){
        int maxint = 0x7fffffff;// max int
        for (int i=0;i<this._count;i++){
            RID rid = new RID(new PageId(-1), -1);// invalid page, should be replaced later
            vac[i] = new VACandidate( maxint, rid);
             
        }
        return maxint;
    }
    private int Candidate (int realdst, RID rid, Vector100Dtype v, Tuple tuple){
        if (realdst < vac[0].getDst())
        {
            vac[0] = new VACandidate(realdst, rid,v,tuple);
            sortCandidate();
        }
        return vac[0].getDst();
         
    }
    private void sortCandidate(){
        VACandidate tmpc = null;
         
        for (int i=0;i<this._count-1;i++){
            for (int j=i+1;j<this._count;j++){
                if (vac[i].getDst() < vac[j].getDst())
                {
                    tmpc = vac[i];
                    vac[i] = vac[j];
                    vac[j] = tmpc;
                }
            }
        }
         
    }
         
 
}