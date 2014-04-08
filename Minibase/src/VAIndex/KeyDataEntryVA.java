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
//	public KeyDataEntryVA(Vector100Key key){
//		this.key = key;
//		RID r = new RID(new PageId(-1),-1);
//		this.rid = r;
//	}
	/**
	 * construct from bytes 4-7
	 * @param data
	 * @param b
	 * @throws VAException
	 * @throws IOException
	 */
	public KeyDataEntryVA(byte[] data, int b) throws VAException, IOException{
		this.key = new Vector100Key(b);
		key.setDataBytes(data,0);
		int pid;
		int slotNo;
		int keylen=Vector100Key.getVAKeyLength(b);
		
		slotNo = Convert.getIntValue(keylen, data);
		pid = Convert.getIntValue(keylen+4, data);
		PageId pageid = new PageId(pid);
		this.rid = new RID(pageid,slotNo);	
	}

	
	public Vector100Key getKey() {
		return key;
	}
	public RID getRid() {
		if (rid == null)
			System.out.println("error rid is null in getRid()");
		return rid;
	}
	
	/**
	 * convert from data to bytes 4-7
	 * @return
	 * @throws IOException
	 */
	byte [] getBytesFromEntry() throws IOException{
		int datalength = key.getDataLength()+8;// vector + int
		byte[] data = new byte[datalength];

		Convert.set100DVectorKeyValue(this.key, 0, data);
		Convert.setIntValue(this.rid.slotNo, datalength-8, data);
		Convert.setIntValue(this.rid.pageNo.pid, datalength-4, data);
		
		return data;
	}
	/**
	 * convert from bytes to data 4-7
	 * @param data
	 * @param b 
	 * @return
	 * @throws VAException
	 * @throws IOException
	 */
	public static KeyDataEntryVA getEntryFromBytes(byte[] data, int b) throws VAException, IOException{
		Vector100Key vkey = new Vector100Key(b);
		vkey.setDataBytes(data,0);
		int pid;
		int slotNo;
		int keylen=Vector100Key.getVAKeyLength(b);
		slotNo = Convert.getIntValue(keylen, data);
		pid = Convert.getIntValue(keylen+4, data);
		PageId pageid = new PageId(pid);
		RID rid = new RID(pageid,slotNo);
		KeyDataEntryVA kde = new KeyDataEntryVA(vkey,rid);
		
		return kde;
	}
	

}
