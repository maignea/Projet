package com.runmycode.mapred;

public class Event 
{	
	public enum Type
	{
		KILLED,
		FAILURE_BUG,
		FAILURE_SYSTEM,
		FAILURE_TIMEOUT_QUEUE,
		FAILURE_TIMEOUT_WORKER
		// FAILURE ?
	}

	Type type;
	Object source;
	
	public Event(Type type, Object source) 
	{
		super();
		this.type = type;
		this.source = source;
	}

	public Type getType()
	{
		return type;
	}

	public Object getSource() 
	{
		return source;
	}	
}
