package com.runmycode.mapred;

import java.io.Serializable;

public class JobID extends ID implements Serializable, Comparable
{
	/**
	 * Constructs a JobID object 
	 * @param jtIdentifier jobTracker identifier
	 * @param id job number
	 */
	public JobID(String id)
	{
		super(id);
	}
	
	public JobID() 
	{
		
	}
		
	/** Construct a JobId object from given string 
	 * @return constructed JobId object or null if the given String is null
	 * @throws IllegalArgumentException if the given string is malformed
	 */
	public static JobID forName(String str) throws IllegalArgumentException
	{
		if(str == null)
		{
			throw new IllegalArgumentException("str shloud not be null");
		}
		return new JobID(str);		
	}
	
	@Override
	public int compareTo(Object object) 
	{
		JobID jodID = (JobID)object;
		return id.compareTo(jodID.id);
	}
}
