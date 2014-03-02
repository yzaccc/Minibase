package VAIndex;

import chainexception.ChainException;

public class VAException  extends ChainException{
	public VAException()
	{
		super();
	}
	public VAException(Exception ex, String name)
	{
		super(ex, name); 
	}

}
