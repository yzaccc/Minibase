package global;

public interface GlobalConst {

  public static  int MINIBASE_MAXARRSIZE = 50;
  //public static  int NUMBUF = 50;
  public static  int NUMBUF = 4000;

  /** Size of page. */
  public static  int MINIBASE_PAGESIZE = 1024;           // in bytes

  /** Size of each frame. */
  //public static  int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames
  public static  int MINIBASE_BUFFER_POOL_SIZE = 4096;

  public static  int MAX_SPACE = 1024;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static  int MINIBASE_DB_SIZE = 10000;           
  public static  int MINIBASE_MAX_TRANSACTIONS = 100;
  public static  int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static  int MAXFILENAME  = 15;          
  public static  int MAXINDEXNAME = 40;
  public static  int MAXATTRNAME  = 15;    
  public static  int MAX_NAME = 50;

  public static  int INVALID_PAGE = -1;
}
