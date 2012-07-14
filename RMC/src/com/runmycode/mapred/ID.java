package com.runmycode.mapred;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * A general identifier, which internally stores the id
 * as an integer. This is the super class of {@link JobID}, 
 * {@link TaskID} and {@link TaskInProgressID}.
 * 
 * @see JobID
 * @see TaskID
 * @see TaskInProgressID
 */
public class ID implements Serializable
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.ID.class);
	protected String id;
	
	/** 
	 * Constructs an ID object from the given int.
	 */
	public ID(String id)
	{
		this.id = id; 
	}
	
	protected ID()
	{
		
	}
	
	public String getId()
	{
		return id;
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		logger.debug("equals dans ID");
		
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ID other = (ID) obj;
		if (id == null)
		{
			if (other.id != null)
				return false;
		} 
		else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	@Override
	public String toString() 
	{
		return this.id;
	}
}
