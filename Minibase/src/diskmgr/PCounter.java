package diskmgr;

public class PCounter {
	  public static int counter = 0;
	  public static boolean initialized = false;

//	  public static void initialize() {
//	      if(initialized == false){
//	    	  counter = 0;
//	      }
//	      initialized = true;
//		}
	  public static void increment() {
	      counter++;
	    }
	  public static void setZero(){
		  counter = 0;
	  }
}
