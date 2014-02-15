package global;
//qwe
public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 50;
  /**
   * Zongkun
   */
  //public static final int NUMBUF = 50;
  public static final int NUMBUF = 4000;

  /** Size of page. */
  /**
   * Zongkun
   */
  //public static final int MINIBASE_PAGESIZE = 1024;           // in bytes
  public static final int MINIBASE_PAGESIZE = 4096;
  /** Size of each frame. */
  /**
   * Zongkun
   */
  //public static final int MINIBASE_BUFFER_POOL_SIZE = 1024;
  public static final int MINIBASE_BUFFER_POOL_SIZE = 4096;   // in Frames
  /**
   * Zongkun
   */
  public static final int MAX_SPACE = 1024;   // in Frames
//  public static final int MAX_SPACE = 4096;
   
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 10000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    
  public static final int MAX_NAME = 50;

  public static final int INVALID_PAGE = -1;
}
