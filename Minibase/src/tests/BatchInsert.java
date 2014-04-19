package tests;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

class BatchInsertDriver extends TestDriver {
	public BatchInsertDriver()
	{
	super("");
	}
	
	public boolean runTest(String updatefilename, String relname){
		boolean success = false;
		System.out.println(updatefilename);
		System.out.println(relname);
		
		BufferedReader fileReader = null;
		// brStr is used to store on line read from br.
		String brStr = null;
		String[] brStrArray;
		try
		{
			br = new BufferedReader(new FileReader(updatefilename));
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		return success;
	}
}
public class BatchInsert
{
	public static void main (String argv[])
	{
	boolean insertStatus = false;
	BatchInsertDriver batchInsert = new BatchInsertDriver();
	insertStatus = batchInsert.runTest(argv[0], argv[1]);
	if (insertStatus == false)
	{
		System.out.print("Batch Insert Failed.\n");
	}
	else
	{
		System.out.print("Bathch Insert Success.\n");
	}
	}
}
