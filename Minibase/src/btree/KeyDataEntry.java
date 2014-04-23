/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;

import VAIndex.Vector100Key;
import global.*;

/**
 * KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
	/**
	 * key in the (key, data)
	 */
	public KeyClass key;
	/**
	 * data in the (key, data)
	 */
	public DataClass data;

	/**
	 * Class constructor
	 */
	public KeyDataEntry(Integer key, PageId pageNo) {
		this.key = new IntegerKey(key);
		this.data = new IndexData(pageNo);
	};

	/**
	 * Class constructor.
	 * 
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry(KeyClass key, PageId pageNo)
			throws KeyNotMatchException {

		data = new IndexData(pageNo);
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());
		else if (key instanceof Vector100Key) {
			this.key = new Vector100Key(
					((Vector100Key) key).getDataLength(),
					((Vector100Key) key).getData());
		}
		else
			throw new KeyNotMatchException(null, "key types do not match");
	};

	/**
	 * Class constructor.
	 */
	public KeyDataEntry(String key, PageId pageNo) {
		this.key = new StringKey(key);
		this.data = new IndexData(pageNo);
	};

	/**
	 * Class constructor.
	 */
	public KeyDataEntry(Integer key, RID rid) {
		this.key = new IntegerKey(key);
		this.data = new LeafData(rid);
	};

	/**
	 * Class constructor.
	 * 
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry(KeyClass key, RID rid) throws KeyNotMatchException {
		data = new LeafData(rid);
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());
		else if (key instanceof Vector100Key) {
//			 ((Vector100Key) key).psrintDebug();
			this.key = new Vector100Key(
					((Vector100Key) key).getDataLength(),
					((Vector100Key) key).getData());
//			try {
//				// this.key = new Vector100Key(((Vector100Key)
//				// key).get_vector(),
//				// ((Vector100Key) key).get_b());
//				this.key = new Vector100Key(
//						((Vector100Key) key).getDataLength(),
//						((Vector100Key) key).getData());
//			} catch (Exception e) {
//				
//				for (StackTraceElement ste : Thread.currentThread()
//						.getStackTrace()) {
//					System.out.println("in keyDataEntry*** "+e.getMessage());
//					System.out.println(ste);
//				}
//			}

		} else
			throw new KeyNotMatchException(null, "key types do not match");
	};

	/**
	 * Class constructor.
	 */
	public KeyDataEntry(String key, RID rid) {
		this.key = new StringKey(key);
		this.data = new LeafData(rid);
	};

	/**
	 * Class constructor.
	 * 
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry(KeyClass key, DataClass data)
			throws KeyNotMatchException {
		if (key instanceof IntegerKey)
			this.key = new IntegerKey(((IntegerKey) key).getKey());
		else if (key instanceof StringKey)
			this.key = new StringKey(((StringKey) key).getKey());
		else if (key instanceof Vector100Key) {
				this.key = new Vector100Key(
						((Vector100Key) key).getDataLength(),
						((Vector100Key) key).getData());
		} else
			throw new KeyNotMatchException(null, "key types do not match");

		if (data instanceof IndexData)
			this.data = new IndexData(((IndexData) data).getData());
		else if (data instanceof LeafData)
			this.data = new LeafData(((LeafData) data).getData());
	}

	/**
	 * shallow equal.
	 * 
	 * @param entry
	 *            the entry to check again key.
	 * @return true, if entry == key; else, false.
	 * @throws KeyNotMatchException
	 */
	public boolean equals(KeyDataEntry entry) throws KeyNotMatchException {
		boolean st1, st2;

		if (key instanceof IntegerKey)
			st1 = ((IntegerKey) key).getKey().equals(
					((IntegerKey) entry.key).getKey());
		else if (key instanceof StringKey)
			st1 = ((StringKey) key).getKey().equals(
					((StringKey) entry.key).getKey());
		else if (key instanceof Vector100Key) {
			Vector100Key k1 = (Vector100Key) key;
			Vector100Key k2 = (Vector100Key) entry.key;
			if (Vector100Key.keyCompare(k1, k2) == 0)
				st1 = true;
			else
				st1 = false;
		} else
			throw new KeyNotMatchException(null, "key types do not match");

		if (data instanceof IndexData)
			st2 = ((IndexData) data).getData().pid == ((IndexData) entry.data)
					.getData().pid;
		else
			st2 = ((RID) ((LeafData) data).getData())
					.equals(((RID) ((LeafData) entry.data).getData()));

		return (st1 && st2);
	}
}
