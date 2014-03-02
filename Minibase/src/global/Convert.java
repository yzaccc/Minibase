/* file Convert.java */

package global;

import java.io.*;
import java.lang.*;
import java.util.Arrays;

import VAIndex.KeyDataEntryVA;
import VAIndex.VAException;
import VAIndex.Vector100Key;

public class Convert{
 /**
  * Zongkun
  * Task 2
  * read from given byte array at the specified position convert it to a Vector100Dtype
  * @param position
  * @param data
  * @return
 * @throws IOException 
 * @throws VAException 
  */
public static KeyDataEntryVA get100DVectorKeyValue(int position, byte[] data, int length) throws IOException, VAException{

	  KeyDataEntryVA keydata;
	  int b = (length-8)*8/100;
	  byte tmp[] = new byte[length];
	  System.arraycopy (data, position, tmp, 0, length);
	  keydata = new KeyDataEntryVA(tmp,b);
//	  System.out.println("int Convert get100DVectorKeyValue length="+length);//debug
//	  System.out.println(Arrays.toString(tmp));//debug
//	  keydata=new KeyDataEntryVA();
//	  System.out.println("in get100DVectorKeyValue position = "+ position);//debug
//	  keydata
//	  //.setDataBytes(data, position+8);//??? not sure need arrycopy
	  return keydata;
}
public static Vector100Dtype get100DVectorValue(int position, byte[] data) throws IOException{
	  //InputStream in;
    //DataInputStream instr;
    Vector100Dtype value;
    byte tmp[] = new byte[200];
    
    // copy the value from data array out to a tmp byte array
    System.arraycopy (data, position, tmp, 0, 200);
    //System.out.println("data in get100DVectorValue\n"+Arrays.toString(data));//debug
    //System.out.println("hello4");
    
    value=new Vector100Dtype(tmp);
    /* creates a new data input stream to read data from the
     * specified input stream
     */
//    in = new ByteArrayInputStream(tmp);
//    instr = new DataInputStream(in);
//    value = instr.readInt();  
    //System.out.println("in get100DVectorValue");
    //value.printVector();//debug
    return value;
}
 /**
  * Zongkun 
  * Task 2
  * update a Vector100Dtype in the given byte array at the specified position
  * @param value
  * @param position
  * @param data
  * @return
 * @throws IOException 
  */
public static void set100DVectorValue(Vector100Dtype value, int position, byte[] data) throws IOException{

//   Vector100Dtype v=new Vector100Dtype(data);
//    
//    // creates a byte array with this output stream size and the
//    // valid contents of the buffer have been copied into it
//    byte []B = ((ByteArrayOutputStream) out).toByteArray();
//    
//    // copies the first 2 bytes of this byte array into data[] 

	System.arraycopy (Vector100Dtype.Vector100DtoByte(value), 0, data, position, 200);

}
//End

public static void set100DVectorKeyValue(Vector100Key value, int position, byte[] data) throws IOException{

//  Vector100Dtype v=new Vector100Dtype(data);
//   
//   // creates a byte array with this output stream size and the
//   // valid contents of the buffer have been copied into it
//   byte []B = ((ByteArrayOutputStream) out).toByteArray();
//   
//   // copies the first 2 bytes of this byte array into data[] 

	System.arraycopy (value.returnKeyByteArray(), 0, data, position, value.getDataLength());

}
 /**
 * read 4 bytes from given byte array at the specified position
 * convert it to an integer
 * @param  	data 		a byte array 
 * @param       position  	in data[]
 * @exception   java.io.IOException I/O errors
 * @return      the integer 
 */
  public static int getIntValue (int position, byte []data)
   throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      int value;
      byte tmp[] = new byte[4];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 4);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readInt();  
      
      return value;
    }
  
  /**
   * read 4 bytes from given byte array at the specified position
   * convert it to a float value
   * @param  	data 		a byte array 
   * @param       position  	in data[]
   * @exception   java.io.IOException I/O errors
   * @return      the float value
   */
  public static float getFloValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      float value;
      byte tmp[] = new byte[4];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 4);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readFloat();  
      
      return value;
    }
  
  
  /**
   * read 2 bytes from given byte array at the specified position
   * convert it to a short integer
   * @param  	data 		a byte array
   * @param       position  	the position in data[]
   * @exception   java.io.IOException I/O errors
   * @return      the short integer
   */
  public static short getShortValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      short value;
      byte tmp[] = new byte[2];
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, 2);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readShort();
      
      return value;
    }
  
  /**
   * reads a string that has been encoded using a modified UTF-8 format from
   * the given byte array at the specified position
   * @param       data            a byte array
   * @param       position        the position in data[]
   * @param 	length 		the length of the string in bytes
   *			         (=strlength +2)
   * @exception   java.io.IOException I/O errors
   * @return      the string
   */
  public static String getStrValue (int position, byte []data, int length)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      String value;
      byte tmp[] = new byte[length];  
      
      // copy the value from data array out to a tmp byte array
      System.arraycopy (data, position, tmp, 0, length);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readUTF();
      return value;
    }
  
  /**
   * reads 2 bytes from the given byte array at the specified position
   * convert it to a character
   * @param       data            a byte array
   * @param       position        the position in data[]
   * @exception   java.io.IOException I/O errors
   * @return      the character
   */
  public static char getCharValue (int position, byte []data)
    throws java.io.IOException
    {
      InputStream in;
      DataInputStream instr;
      char value;
      byte tmp[] = new byte[2];
      // copy the value from data array out to a tmp byte array  
      System.arraycopy (data, position, tmp, 0, 2);
      
      /* creates a new data input stream to read data from the
       * specified input stream
       */
      in = new ByteArrayInputStream(tmp);
      instr = new DataInputStream(in);
      value = instr.readChar();
      return value;
    }
  
  
  /**
   * update an integer value in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into the data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setIntValue (int value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeInt(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 4 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 4);
      
    }
  
  /**
   * update a float value in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into the data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setFloValue (float value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeFloat(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 4 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 4);
      
    }
  
  /**
   * update a short integer in the given byte array at the specified position
   * @param  	data 		a byte array
   * @param	value   	the value to be copied into data[]
   * @param	position  	the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setShortValue (short value, int position, byte []data) 
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to 
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      
      outstr.writeShort(value);
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies the first 2 bytes of this byte array into data[] 
      System.arraycopy (B, 0, data, position, 2);
      
    }
  
  /**
   * Insert or update a string in the given byte array at the specified 
   * position.
   * @param       data            a byte array
   * @param       value           the value to be copied into data[]
   * @param       position        the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
 public static void setStrValue (String value, int position, byte []data)
        throws java.io.IOException
 {
  /* creates a new data output stream to write data to
   * underlying output stream
   */
 
   OutputStream out = new ByteArrayOutputStream();
   DataOutputStream outstr = new DataOutputStream (out);
   
   // write the value to the output stream
   
   outstr.writeUTF(value);
   // creates a byte array with this output stream size and the 
   // valid contents of the buffer have been copied into it
   byte []B = ((ByteArrayOutputStream) out).toByteArray();
   
   int sz =outstr.size();  
   // copies the contents of this byte array into data[]
   System.arraycopy (B, 0, data, position, sz);
   
 }
  
  /**
   * Update a character in the given byte array at the specified position.
   * @param       data            a byte array
   * @param       value           the value to be copied into data[]
   * @param       position        the position of tht value in data[]
   * @exception   java.io.IOException I/O errors
   */
  public static void setCharValue (char value, int position, byte []data)
    throws java.io.IOException
    {
      /* creates a new data output stream to write data to
       * underlying output stream
       */
      
      OutputStream out = new ByteArrayOutputStream();
      DataOutputStream outstr = new DataOutputStream (out);
      
      // write the value to the output stream
      outstr.writeChar(value);  
      
      // creates a byte array with this output stream size and the
      // valid contents of the buffer have been copied into it
      byte []B = ((ByteArrayOutputStream) out).toByteArray();
      
      // copies contents of this byte array into data[]
      System.arraycopy (B, 0, data, position, 2);
      
    }
}
