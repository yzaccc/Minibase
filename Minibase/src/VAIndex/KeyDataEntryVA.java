package VAIndex;

import java.io.IOException;

import global.Convert;
import global.PageId;
import global.RID;

public class KeyDataEntryVA {
	
	public Vector100Key key;
	public RID rid;
	
	
	public KeyDataEntryVA(Vector100Key key, RID rid){
		this.key = key;
		this.rid = rid;
	}
	public KeyDataEntryVA(byte[] data, int b) throws VAException, IOException{
		this.key = new Vector100Key(b);
		key.setDataBytes(data,8);
		int pid;
		int slotNo;
		slotNo = Convert.getIntValue(0, data);
		pid = Convert.getIntValue(4, data);
		PageId pageid = new PageId(pid);
		this.rid = new RID(pageid,slotNo);	
	}

	
	public Vector100Key getKey() {
		return key;
	}
	public RID getRid() {
		return rid;
	}
	byte [] getBytesFromEntry() throws IOException{
		int datalength = key.getDataLength()+8;// vector + int
		byte[] data = new byte[datalength];
		Convert.setIntValue(rid.slotNo, 0, data);
		Convert.setIntValue(rid.pageNo.pid, 4, data);
		Convert.set100DVectorKeyValue(key, 8, data);
		return data;
	}
	
	public static KeyDataEntryVA getEntryFromBytes(byte[] data, int b) throws VAException, IOException{
		Vector100Key vkey = new Vector100Key(b);
		vkey.setDataBytes(data,8);
		int pid;
		int slotNo;
		slotNo = Convert.getIntValue(0, data);
		pid = Convert.getIntValue(4, data);
		PageId pageid = new PageId(pid);
		RID rid = new RID(pageid,slotNo);
		KeyDataEntryVA kde = new KeyDataEntryVA(vkey,rid);
		
		return kde;
	}
	

}
